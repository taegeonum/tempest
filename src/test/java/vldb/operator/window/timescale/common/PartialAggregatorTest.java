package vldb.operator.window.timescale.common;

import junit.framework.Assert;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Test;
import vldb.operator.window.aggregator.CAAggregator;
import vldb.operator.window.aggregator.impl.CountByKeyAggregator;
import vldb.operator.window.aggregator.impl.KeyExtractor;
import vldb.operator.window.timescale.Timescale;
import vldb.operator.window.timescale.parameter.StartTime;
import vldb.test.util.IntegerExtractor;

import java.util.*;
import java.util.concurrent.CountDownLatch;

import static org.mockito.Mockito.*;

/**
 * Created by taegeonum on 4/21/16.
 */
public final class PartialAggregatorTest {

  @Test
  public void testPartialAggregator() throws InjectionException, InterruptedException {
    final List<List<Timespan>> result = new LinkedList<>();
    final SpanTracker<Map<Integer, Long>> spanTracker = mock(SpanTracker.class);
    final CountDownLatch countDownLatch = new CountDownLatch(3);
    final FinalAggregator<Map<Integer, Long>> finalAggregator = new FinalAggregator<Map<Integer, Long>>() {
      @Override
      public void triggerFinalAggregation(final List<Timespan> finalTimespans, final long r) {
        result.add(finalTimespans);
        countDownLatch.countDown();
      }

      @Override
      public void close() throws Exception {

      }
    };
    final Timescale ts1 = new Timescale(4, 2);
    final Timescale ts2 = new Timescale(5, 2);
    final Timescale ts3 = new Timescale(6, 3);
    final List<Timespan> finalTimespans1 = Arrays.asList(new Timespan(-2, 2, ts1), new Timespan(-3, 2, ts2));
    final List<Timespan> finalTimespans2 = Arrays.asList(new Timespan(0, 4, ts1), new Timespan(-3, 2, ts2));
    final List<Timespan> finalTimespans3 = Arrays.asList(new Timespan(0, 2, ts2), new Timespan(-3, 2, ts3));

    when(spanTracker.getNextSliceTime(0L)).thenReturn(2L);
    when(spanTracker.getFinalTimespans(2L)).thenReturn(finalTimespans1);

    when(spanTracker.getNextSliceTime(2L)).thenReturn(3L);
    when(spanTracker.getFinalTimespans(3L)).thenReturn(finalTimespans2);

    when(spanTracker.getNextSliceTime(3L)).thenReturn(4L);
    when(spanTracker.getFinalTimespans(4L)).thenReturn(finalTimespans3);

    when(spanTracker.getNextSliceTime(4L)).thenReturn(6L);
    when(spanTracker.getNextSliceTime(6L)).thenReturn(7L);


    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindImplementation(KeyExtractor.class, IntegerExtractor.class);
    jcb.bindImplementation(CAAggregator.class, CountByKeyAggregator.class);
    jcb.bindNamedParameter(StartTime.class, Long.toString(0));

    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    injector.bindVolatileInstance(SpanTracker.class, spanTracker);
    injector.bindVolatileInstance(FinalAggregator.class, finalAggregator);
    final PartialAggregator<Integer> partialAggregator = injector.getInstance(DefaultPartialAggregator.class);

    countDownLatch.await();
    verify(spanTracker).putAggregate(new HashMap<Integer, Long>(), new Timespan(0, 2, null));
    verify(spanTracker).putAggregate(new HashMap<Integer, Long>(), new Timespan(2, 3, null));
    verify(spanTracker).putAggregate(new HashMap<Integer, Long>(), new Timespan(3, 4, null));

    Assert.assertEquals(finalTimespans1, result.get(0));
    Assert.assertEquals(finalTimespans2, result.get(1));
    Assert.assertEquals(finalTimespans3, result.get(2));
  }
}
