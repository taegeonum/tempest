package vldb.operator.window.timescale.pafas;

import org.apache.reef.tang.*;
import org.junit.Test;
import vldb.evaluation.Metrics;
import vldb.example.DefaultExtractor;
import vldb.operator.window.aggregator.impl.CountByKeyAggregator;
import vldb.operator.window.aggregator.impl.KeyExtractor;
import vldb.operator.window.timescale.TimeWindowOutputHandler;
import vldb.operator.window.timescale.TimescaleWindowOperator;
import vldb.operator.window.timescale.cutty.CuttyMWOConfiguration;
import vldb.operator.window.timescale.pafas.active.ActiveDPSelectionAlgorithm;
import vldb.operator.window.timescale.pafas.active.PruningParallelMaxDependencyGraphImpl;
import vldb.operator.window.timescale.pafas.event.WindowTimeEvent;
import vldb.operator.window.timescale.parameter.*;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.logging.Logger;

/**
 * Created by taegeonum on 4/22/16.
 */
public final class PafasMWOTest {
  private static final Logger LOG = Logger.getLogger(PafasMWOTest.class.getName());

  @Test
  public void testPafasMWO() throws Exception {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindImplementation(KeyExtractor.class, DefaultExtractor.class);
    jcb.bindNamedParameter(NumThreads.class, "4");
    jcb.bindNamedParameter(ReusingRatio.class, "0.0");
    jcb.bindNamedParameter(WindowGap.class, "25");
    jcb.bindNamedParameter(SharedFinalNum.class, "4000");
    jcb.bindNamedParameter(OverlappingRatio.class, "0.0");

    final long currTime = 0;
    final List<Configuration> configurationList = new LinkedList<>();
    final List<String> operatorIds = new LinkedList<>();
    final String timescaleString3 =  "(4,2)(5,3)(6,4)(10,5)";
    final String timescaleString1 =  "(5,4)(8,3)(12,7)(16,6)";
    final String timescaleString2 = "(5,1)(10,1)(20,2)(30,2)(60,4)(90,4)(360,5)(600,5)(900,10)(1800,10)";
    final String timescaleString4 = "(5,2)(6,2)(10,2)";
    final String timescaleString5 = "(107,60)(170,3)(935,10)(1229,10)(1991,110)(2206,20)(2284,140)(2752,30)(2954,88)(2961,165)(2999,60)(3043,55)(3076,35)(3134,110)(3161,210)(3406,40)(3515,385)(3555,40)(3590,210)(3593,840)";
    final String timescaleString = "(107,60)(170,3)(179,11)(411,15)(656,15)(868,140)(886,15)(915,40)(935,10)(1229,10)(1396,20)(1430,20)(1828,60)(1991,110)(2032,22)(2150,30)(2206,20)(2262,120)(2284,140)(2344,40)(2534,140)(2752,30)(2812,40)(2843,35)(2954,88)(2961,165)(2999,60)(3027,42)(3043,55)(3076,35)(3110,70)(3134,110)(3148,35)(3161,210)(3166,30)(3289,55)(3303,35)(3317,110)(3404,35)(3406,40)(3409,35)(3450,165)(3515,385)(3530,120)(3543,35)(3555,40)(3558,40)(3566,660)(3590,210)(3593,840)";
    // PAFAS

    /*
    configurationList.add(EagerMWOConfiguration.CONF
        .set(EagerMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
        .set(EagerMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
        .set(EagerMWOConfiguration.SELECTION_ALGORITHM, ActiveDPSelectionAlgorithm.class)
        .set(EagerMWOConfiguration.OUTPUT_LOOKUP_TABLE, DPOutputLookupTableImpl.class)
        .set(EagerMWOConfiguration.START_TIME, "0")
        .build());
    operatorIds.add("FAST-Active");
*/
    configurationList.add(StaticSingleMWOConfiguration.CONF
        .set(StaticSingleMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
        .set(StaticSingleMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
        .set(StaticSingleMWOConfiguration.SELECTION_ALGORITHM, ActiveDPSelectionAlgorithm.class)
        .set(StaticSingleMWOConfiguration.OUTPUT_LOOKUP_TABLE, DPOutputLookupTableImpl.class)
        .set(StaticSingleMWOConfiguration.DEPENDENCY_GRAPH, PruningParallelMaxDependencyGraphImpl.class)
        .set(StaticSingleMWOConfiguration.START_TIME, "0")
        .build());
    operatorIds.add("FAST");

    configurationList.add(CuttyMWOConfiguration.CONF
    .set(CuttyMWOConfiguration.START_TIME, currTime)
    .set(CuttyMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
    .set(CuttyMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
        .build());
    operatorIds.add("Cutty");

/*
    configurationList.add(ActiveDynamicMWOConfiguration.CONF
        .set(ActiveDynamicMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
        .set(ActiveDynamicMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
        .set(ActiveDynamicMWOConfiguration.SELECTION_ALGORITHM, DynamicDPTradeOffSelectionAlgorithm.class)
        .set(ActiveDynamicMWOConfiguration.OUTPUT_LOOKUP_TABLE, DynamicDPOutputLookupTableImpl.class)
        .set(ActiveDynamicMWOConfiguration.DYNAMIC_DEPENDENCY, DynamicOptimizedDependencyGraphImpl.class)
        .set(ActiveDynamicMWOConfiguration.START_TIME, currTime)
        .build());
    operatorIds.add("Dynamic-FAST");
*/

    /*
    // PAFAS-Greedy
    configurationList.add(StaticMWOConfiguration.CONF
        .set(StaticMWOConfiguration.INITIAL_TIMESCALES, "(4,2)(5,3)(6,4)(10,5)")
        .set(StaticMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
        .set(StaticMWOConfiguration.SELECTION_ALGORITHM, DPSelectionAlgorithm.class)
        .set(StaticMWOConfiguration.START_TIME, currTime)
        .build());
    operatorIds.add("PAFAS-DP");



    // On-the-fly operator
    configurationList.add(OntheflyMWOConfiguration.CONF
        .set(OntheflyMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
        .set(OntheflyMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
        .set(OntheflyMWOConfiguration.START_TIME, currTime)
        .build());
    operatorIds.add("OntheFly");

    // TriOPs
    configurationList.add(TriOpsMWOConfiguration.CONF
        .set(TriOpsMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
        .set(TriOpsMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
        .set(TriOpsMWOConfiguration.START_TIME, currTime)
        .build());
    operatorIds.add("TriOps");
*/

  
    int i = 0;
    final List<TimescaleWindowOperator> mwos = new LinkedList<>();
    final List<Metrics> aggregationCounters = new LinkedList<>();
    for (final Configuration conf : configurationList) {
      final Injector injector = Tang.Factory.getTang().newInjector(Configurations.merge(jcb.build(), conf));
      injector.bindVolatileInstance(TimeWindowOutputHandler.class, new LoggingHandler<>(operatorIds.get(i)));
      System.out.println("Creating " + operatorIds.get(i));
      final TimescaleWindowOperator<String, Map<String, Long>> mwo = injector.getInstance(TimescaleWindowOperator.class);
      System.out.println("Finished creation " + operatorIds.get(i));
      mwos.add(mwo);
      final Metrics metrics = injector.getInstance(Metrics.class);
      aggregationCounters.add(metrics);
      i += 1;
    }

    final int numKey = 10;
    final int numInput = 6000;
    final Random random = new Random();
    final int tick = numInput / 3000;
    int tickTime = 1;
    long stored = 0;
    for (i = 0; i < numInput; i++) {
      final int key = Math.abs(random.nextInt()%numKey);
      for (final TimescaleWindowOperator mwo : mwos) {
        if (i % tick == 0) {
          mwo.execute(new WindowTimeEvent(tickTime));
        }
        mwo.execute(Integer.toString(key));
      }

      if (i % tick == 0) {
        tickTime += 1;
      }
    }

    for (final TimescaleWindowOperator mwo : mwos) {
      mwo.close();
    }

    i = 0;
    for (final TimescaleWindowOperator mwo : mwos) {
      final Metrics aggregationCounter = aggregationCounters.get(i);
      final long partialCount = aggregationCounter.partialCount;
      final long finalCount = aggregationCounter.finalCount;
      final long storedAgg = aggregationCounter.storedFinal + aggregationCounter.storedPartial;
      final String id = operatorIds.get(i);
      LOG.info(id + " aggregation count: partial: " + partialCount + ", final: " + finalCount
          + ", total: " + (partialCount + finalCount) + ", stored: " + storedAgg);
      i += 1;
    }
  }
}
