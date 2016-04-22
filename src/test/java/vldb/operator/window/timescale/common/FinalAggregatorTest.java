package vldb.operator.window.timescale.common;

import junit.framework.Assert;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Test;
import vldb.operator.OutputEmitter;
import vldb.operator.window.aggregator.CAAggregator;
import vldb.operator.window.aggregator.impl.CountByKeyAggregator;
import vldb.operator.window.aggregator.impl.KeyExtractor;
import vldb.operator.window.timescale.TimeWindowOutputHandler;
import vldb.operator.window.timescale.Timescale;
import vldb.operator.window.timescale.TimescaleWindowOutput;
import vldb.operator.window.timescale.parameter.StartTime;
import vldb.test.util.IntegerExtractor;

import java.util.*;
import java.util.concurrent.CountDownLatch;

import static org.mockito.Mockito.*;

/**
 * Created by taegeonum on 4/21/16.
 */
public final class FinalAggregatorTest {

  @Test
  public void testFinalAggregator() throws InjectionException, InterruptedException {
    final Timescale ts1 = new Timescale(4, 2);
    final Timescale ts2 = new Timescale(5, 2);
    final CountDownLatch countDownLatch = new CountDownLatch(2);
    final List<Timespan> finalTimespans2 = Arrays.asList(new Timespan(0, 4, ts1), new Timespan(-3, 2, ts2));
    final List<Map<Integer, Long>> result = new LinkedList<>();
    final SpanTracker<Map<Integer, Long>> spanTracker = mock(SpanTracker.class);

    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindImplementation(KeyExtractor.class, IntegerExtractor.class);
    jcb.bindImplementation(CAAggregator.class, CountByKeyAggregator.class);
    jcb.bindNamedParameter(StartTime.class, Long.toString(0));

    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    injector.bindVolatileInstance(SpanTracker.class, spanTracker);
    final TimeWindowOutputHandler<Map<Integer, Long>, Map<Integer, Long>> handler =
        new TimeWindowOutputHandler<Map<Integer, Long>, Map<Integer, Long>>() {
          @Override
          public void execute(final TimescaleWindowOutput<Map<Integer, Long>> val) {
            result.add(val.output.result);
            countDownLatch.countDown();
          }

          @Override
          public void prepare(final OutputEmitter<TimescaleWindowOutput<Map<Integer, Long>>> outputEmitter) {

          }
        };
    injector.bindVolatileInstance(TimeWindowOutputHandler.class, handler);
    final FinalAggregator<Map<Integer, Long>> finalAggregator = injector.getInstance(FinalAggregator.class);
    final CountByKeyAggregator<Map<Integer, Long>, Integer> countByKeyAggregator =
        injector.getInstance(CountByKeyAggregator.class);

    final List<Map<Integer, Long>> input1 = generateMapList(10);
    final List<Map<Integer, Long>> input2 = generateMapList(20);
    when(spanTracker.getDependentAggregates(new Timespan(0, 4, ts1))).thenReturn(input1);
    when(spanTracker.getDependentAggregates(new Timespan(-3, 2, ts2))).thenReturn(input2);

    finalAggregator.triggerFinalAggregation(finalTimespans2);
    countDownLatch.await();
    verify(spanTracker).putAggregate(result.get(0), new Timespan(0, 4, ts1));
    verify(spanTracker).putAggregate(result.get(1), new Timespan(-3, 2, ts1));

    Assert.assertEquals(countByKeyAggregator.aggregate(input1), result.get(0));
    Assert.assertEquals(countByKeyAggregator.aggregate(input2), result.get(1));

  }

  public List<Map<Integer, Long>> generateMapList(final int length) {
    final List<Map<Integer, Long>> result = new LinkedList<>();
    final Random random = new Random();
    for (int i = 0; i < length; i++) {
      final Map<Integer, Long> map = new HashMap<>();
      map.put(random.nextInt() % 10, 1L);
      map.put(random.nextInt() % 10, 1L);
      map.put(random.nextInt() % 10, 1L);
      result.add(map);
    }
    return result;
  }
}
