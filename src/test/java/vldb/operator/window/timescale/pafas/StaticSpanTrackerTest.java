package vldb.operator.window.timescale.pafas;

import junit.framework.Assert;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Test;
import vldb.operator.window.timescale.Timescale;
import vldb.operator.window.timescale.common.SpanTracker;
import vldb.operator.window.timescale.common.TimescaleParser;
import vldb.operator.window.timescale.common.Timespan;
import vldb.operator.window.timescale.parameter.StartTime;
import vldb.operator.window.timescale.parameter.TimescaleString;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public final class StaticSpanTrackerTest {
  private static final Logger LOG = Logger.getLogger(StaticSpanTrackerTest.class.getName());

  @Test
  public void testStaticSpanTracker() throws InjectionException {
    final String tsString = "(4,2)(5,3)(6,3)";
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    final long startTime = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    jcb.bindNamedParameter(TimescaleString.class, tsString);
    jcb.bindNamedParameter(StartTime.class, startTime+"");
    jcb.bindImplementation(DependencyGraph.class, StaticDependencyGraphImpl.class);
    jcb.bindImplementation(DependencyGraph.SelectionAlgorithm.class, GreedySelectionAlgorithm.class);

    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    final SpanTracker<Integer> spanTracker = injector.getInstance(StaticSpanTrackerImpl.class);
    final TimescaleParser tsParser = injector.getInstance(TimescaleParser.class);
    final List<Timescale> timescales = tsParser.timescales;
    final PeriodCalculator periodCalculator = injector.getInstance(PeriodCalculator.class);
    final long period = periodCalculator.getPeriod();

    // Test getNextSliceTime
    // 1, 2, 3, 4, 6 ...
    long prevTime = startTime;
    for (int i = 0; i < 3; i++) {
      final long j = startTime + i * period;
      // 1
      prevTime = spanTracker.getNextSliceTime(prevTime);
      Assert.assertEquals(1 + j, prevTime);
      // 2
      prevTime = spanTracker.getNextSliceTime(prevTime);
      Assert.assertEquals(2 + j, prevTime);
      // 3
      prevTime = spanTracker.getNextSliceTime(prevTime);
      Assert.assertEquals(3 + j, prevTime);
      // 4
      prevTime = spanTracker.getNextSliceTime(prevTime);
      Assert.assertEquals(4 + j, prevTime);
      // 6
      prevTime = spanTracker.getNextSliceTime(prevTime);
      Assert.assertEquals(6 + j, prevTime);
    }

    // Test getFinalTimespans
    // At 2: [-2, 2]
    // At 3: [-2, 3), [-3, 3)
    // At 4: [0, 4)
    // At 6: [2, 6), [1, 6), [0, 6)
    // ...
    for (int i = 0; i < 3; i++) {
      final long j = startTime + i * period;
      // at 2
      final List<Timespan> ts1 = spanTracker.getFinalTimespans(2+j);
      Assert.assertEquals(Arrays.asList(
              new Timespan(-2+j, 2+j, timescales.get(0))),
          ts1);
      // at 3
      final List<Timespan> ts2 = spanTracker.getFinalTimespans(3+j);
      Assert.assertEquals(Arrays.asList(
              new Timespan(-2+j, 3+j, timescales.get(1)),
              new Timespan(-3+j, 3+j, timescales.get(2))),
          ts2);
      // at 4
      final List<Timespan> ts3 = spanTracker.getFinalTimespans(4+j);
      Assert.assertEquals(Arrays.asList(
              new Timespan(j, 4+j, timescales.get(0))),
          ts3);
      // at 6
      final List<Timespan> ts4 = spanTracker.getFinalTimespans(6+j);
      Assert.assertEquals(Arrays.asList(
              new Timespan(2+j, 6+j, timescales.get(0)),
              new Timespan(1+j, 6+j, timescales.get(1)),
              new Timespan(j, 6+j, timescales.get(2))),
          ts4);
    }
  }

  @Test(timeout = 15000L)
  public void testGreedyStaticSpanTrackerGetAggregate() throws InjectionException, InterruptedException {
    final ExecutorService executorService = Executors.newFixedThreadPool(10);
    final long startTime = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());

    final String tsString = "(4,2)(5,3)(6,3)";
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(TimescaleString.class, tsString);
    jcb.bindNamedParameter(StartTime.class, startTime+"");
    jcb.bindImplementation(DependencyGraph.class, StaticDependencyGraphImpl.class);
    jcb.bindImplementation(DependencyGraph.SelectionAlgorithm.class, GreedySelectionAlgorithm.class);

    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    final SpanTracker<Integer> spanTracker = injector.getInstance(StaticSpanTrackerImpl.class);
    final TimescaleParser tsParser = injector.getInstance(TimescaleParser.class);
    final List<Timescale> timescales = tsParser.timescales;
    final PeriodCalculator periodCalculator = injector.getInstance(PeriodCalculator.class);
    final long period = periodCalculator.getPeriod();

    for (int i = 0; i < 50; i++) {
      LOG.info("Iteration: " + (i+1));
      final long j = startTime + i * period;
      // Partial aggregates
      final Timespan p1 = new Timespan(0+j, 1+j, null);
      final Timespan p2 = new Timespan(1+j, 2+j, null);
      final Timespan p3 = new Timespan(2+j, 3+j, null);
      final Timespan p4 = new Timespan(3+j, 4+j, null);
      final Timespan p5 = new Timespan(4+j, 6+j, null);
      spanTracker.putAggregate(1, p1);
      spanTracker.putAggregate(2, p2);

      // [-2, 2): [4, 6) (first outgoing edge), [0, 1), [1, 2)
      final Timespan n1 = new Timespan(-2+j, 2+j, timescales.get(0));
      if (i == 0) {
        Assert.assertEquals(Arrays.asList(
            1, 2
        ), spanTracker.getDependentAggregates(n1));
      } else {
        Assert.assertEquals(Arrays.asList(
            5, 1, 2
        ), spanTracker.getDependentAggregates(n1));
      }

      // [-2, 3): [-2, 2), [2, 3)
      // Test multi-threads
      spanTracker.putAggregate(3, p3);
      final Timespan n2 = new Timespan(-2+j, 3+j, timescales.get(1));
      final List<List<Integer>> result = new LinkedList<>();
      final CountDownLatch countDownLatch = new CountDownLatch(1);
      executorService.submit(new Runnable() {
        @Override
        public void run() {
          final List<Integer> aggregates = spanTracker.getDependentAggregates(n2);
          result.add(aggregates);
          countDownLatch.countDown();
        }
      });
      Thread.sleep(200);
      try {
        spanTracker.putAggregate(8, n1);
      } catch (final Exception e) {
        e.printStackTrace();
      }
      countDownLatch.await();
      Assert.assertEquals(Arrays.asList(8, 3), result.get(0));

      // [-3, 3): [3, 4) (first outgoing edge), [-2, 3)
      final Timespan n3 = new Timespan(-3+j, 3+j, timescales.get(2));
      spanTracker.putAggregate(11, n2);
      if (i == 0) {
        Assert.assertEquals(Arrays.asList(11), spanTracker.getDependentAggregates(n3));
      } else {
        Assert.assertEquals(Arrays.asList(4, 11), spanTracker.getDependentAggregates(n3));
      }

      // [0, 4): p1, p2, p3, p4
      spanTracker.putAggregate(4, p4);
      final Timespan n4 = new Timespan(j, 4+j, timescales.get(0));
      Assert.assertEquals(Arrays.asList(1, 2, 3, 4), spanTracker.getDependentAggregates(n4));
      spanTracker.putAggregate(10, n4);

      // [2, 6): p3, p4, p5
      spanTracker.putAggregate(5, p5);
      final Timespan n5 = new Timespan(2+j, 6+j, timescales.get(0));
      Assert.assertEquals(Arrays.asList(3, 4, 5), spanTracker.getDependentAggregates(n5));
      spanTracker.putAggregate(12, n5);

      // [1, 6): p2, n5
      final Timespan n6 = new Timespan(1+j, 6+j, timescales.get(1));
      Assert.assertEquals(Arrays.asList(2, 12), spanTracker.getDependentAggregates(n6));
      spanTracker.putAggregate(14, n6);

      // [0, 6): n4, p5
      final Timespan n7 = new Timespan(0+j, 6+j, timescales.get(2));
      Assert.assertEquals(Arrays.asList(10, 5), spanTracker.getDependentAggregates(n7));
    }
  }
}
