package atc.operator.window.timescale.pafas;

import org.apache.reef.tang.*;
import org.junit.Test;
import atc.evaluation.parameter.EndTime;
import atc.example.DefaultExtractor;
import atc.operator.window.aggregator.impl.CountByKeyAggregator;
import atc.operator.window.aggregator.impl.KeyExtractor;
import atc.operator.window.timescale.TimeWindowOutputHandler;
import atc.operator.window.timescale.Timescale;
import atc.operator.window.timescale.TimescaleWindowOperator;
import atc.operator.window.timescale.common.TimescaleParser;
import atc.operator.window.timescale.onthefly.OntheflyMWOConfiguration;
import atc.operator.window.timescale.pafas.dynamic.*;
import atc.operator.window.timescale.pafas.event.WindowTimeEvent;
import atc.operator.window.timescale.parameter.NumThreads;
import atc.operator.window.timescale.profiler.AggregationCounter;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.logging.Logger;

/**
 * Created by taegeonum on 4/22/16.
 */
public final class DynamicMWOTEst {
  private static final Logger LOG = Logger.getLogger(DynamicMWOTEst.class.getName());

  @Test
  public void testPafasMWO() throws Exception {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindImplementation(KeyExtractor.class, DefaultExtractor.class);
    jcb.bindNamedParameter(NumThreads.class, "4");

    final long currTime = 0;

    final List<Configuration> configurationList = new LinkedList<>();
    final List<String> operatorIds = new LinkedList<>();

    /*
    configurationList.add(NaiveMWOConfiguration.CONF
        .set(NaiveMWOConfiguration.INITIAL_TIMESCALES, "(30,3)")
        .set(NaiveMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
        .set(NaiveMWOConfiguration.START_TIME, currTime)
        .build());
    operatorIds.add("Naive");
    */


    configurationList.add(OntheflyMWOConfiguration.CONF
        .set(OntheflyMWOConfiguration.INITIAL_TIMESCALES, "(30,3)")
        .set(OntheflyMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
        .set(OntheflyMWOConfiguration.START_TIME, currTime)
        .build());
    operatorIds.add("OnTheFly");

    /*
    configurationList.add(DynamicMWOConfiguration.CONF
        .set(DynamicMWOConfiguration.INITIAL_TIMESCALES, "(30,3)(60,6)(120,12)(80,7)(50,7)(180,18)(300,30)(600,60)(900,90)(1200,120)(1500,150)(1800,180)")
        .set(DynamicMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
        .set(DynamicMWOConfiguration.SELECTION_ALGORITHM, DynamicDPSelectionAlgorithm.class)
        .set(DynamicMWOConfiguration.OUTPUT_LOOKUP_TABLE, DynamicDPOutputLookupTableImpl.class)
        .set(DynamicMWOConfiguration.DYNAMIC_DEPENDENCY, DynamicDependencyGraphImpl.class)
        .set(DynamicMWOConfiguration.DYNAMIC_PARTIAL, DynamicPartialTimespansImpl.class)
        .set(DynamicMWOConfiguration.START_TIME, currTime)
        .build());
    operatorIds.add("Dynamic");
*/

    configurationList.add(DynamicMWOConfiguration.CONF
        .set(DynamicMWOConfiguration.INITIAL_TIMESCALES, "(30,3)")
        .set(DynamicMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
        .set(DynamicMWOConfiguration.SELECTION_ALGORITHM, DynamicGreedySelectionAlgorithm.class)
        .set(DynamicMWOConfiguration.OUTPUT_LOOKUP_TABLE, DynamicGreedyOutputLookupTableImpl.class)
        .set(DynamicMWOConfiguration.DYNAMIC_DEPENDENCY, DynamicOptimizedDependencyGraphImpl.class)
        .set(DynamicMWOConfiguration.DYNAMIC_PARTIAL, DynamicOptimizedPartialTimespans.class)
        .set(DynamicMWOConfiguration.START_TIME, currTime)
        .build());
    operatorIds.add("Dynamic-OPT");

    int hn = 0;
    final List<TimescaleWindowOperator> mwos = new LinkedList<>();
    final List<AggregationCounter> aggregationCounters = new LinkedList<>();
    for (final Configuration conf : configurationList) {
      final Injector injector = Tang.Factory.getTang().newInjector(Configurations.merge(jcb.build(), conf));
      injector.bindVolatileInstance(TimeWindowOutputHandler.class, new LoggingHandler<>(operatorIds.get(hn)));
      injector.bindVolatileParameter(EndTime.class, 500L);
      final TimescaleWindowOperator<Object, Map<String, Long>> mwo = injector.getInstance(TimescaleWindowOperator.class);
      mwos.add(mwo);
      final AggregationCounter aggregationCounter = injector.getInstance(AggregationCounter.class);
      aggregationCounters.add(aggregationCounter);
      hn += 1;
    }

    final List<Timescale> timescaleList = TimescaleParser.parseFromStringNoSort("(30,3)(60,6)(120,12)(80,7)(50,7)(180,18)(300,30)(600,60)(900,90)(1200,120)(1500,150)(1800,180)");
    System.out.println(timescaleList);
    final int numKey = 1000;
    final int numInput = 30000;
    final Random random = new Random();
    long tick = 1;
    for (int i = 1; i <= numInput; i++) {
      final int key = Math.abs(random.nextInt()%numKey);
      long cTickTime = System.nanoTime();
      if (i % 100 == 0) {
        System.out.println("Tick " + tick);
        for (final TimescaleWindowOperator mwo : mwos) {
          mwo.execute(new WindowTimeEvent(tick));
        }

        // Add window

        if (tick <= 90 && tick % 10 == 0) {
          final int index = (int)tick / 10;
          System.out.println("ADD WIndow " + tick + ", " + timescaleList.get(index));
          for (final TimescaleWindowOperator mwo : mwos) {
            mwo.addWindow(timescaleList.get(index), tick);
          }
        }




        if (tick > 90 && tick <= 180 && tick % 10 == 0) {
          int index = 9 - ((int)(tick/10)%10);
          final Timescale ts = timescaleList.get(index);
          System.out.println("RM WIndow " + tick + ", " + ts);
          final long rmStartTime = System.nanoTime();
          for (final TimescaleWindowOperator mwo : mwos) {
            mwo.removeWindow(ts, tick);
          }
        }


        tick += 1;
      } else {
        for (final TimescaleWindowOperator mwo : mwos) {
          mwo.execute(Integer.toString(key));
        }
      }
      Thread.sleep(1);
    }

    for (final TimescaleWindowOperator mwo : mwos) {
      mwo.close();
    }
  }
}
