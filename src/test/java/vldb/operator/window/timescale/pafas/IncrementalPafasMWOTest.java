package vldb.operator.window.timescale.pafas;

import org.apache.reef.tang.*;
import org.junit.Test;
import vldb.evaluation.parameter.EndTime;
import vldb.example.DefaultExtractor;
import vldb.operator.window.aggregator.impl.CountByKeyAggregator;
import vldb.operator.window.aggregator.impl.KeyExtractor;
import vldb.operator.window.timescale.TimeWindowOutputHandler;
import vldb.operator.window.timescale.TimescaleWindowOperator;
import vldb.operator.window.timescale.pafas.event.WindowTimeEvent;
import vldb.operator.window.timescale.parameter.NumThreads;
import vldb.operator.window.timescale.profiler.AggregationCounter;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Created by taegeonum on 4/22/16.
 */
public final class IncrementalPafasMWOTest {
  private static final Logger LOG = Logger.getLogger(IncrementalPafasMWOTest.class.getName());

  @Test
  public void testPafasMWO() throws Exception {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindImplementation(KeyExtractor.class, DefaultExtractor.class);
    jcb.bindNamedParameter(NumThreads.class, "4");

    final long currTime = 0;
    final List<Configuration> configurationList = new LinkedList<>();
    final List<String> operatorIds = new LinkedList<>();

    // PAFAS-Greedy-incremental
    /*
    configurationList.add(StaticMWOConfiguration.CONF
        .set(StaticMWOConfiguration.INITIAL_TIMESCALES, "(4,2)(5,3)(6,4)(10,5)")
        .set(StaticMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
        .set(StaticMWOConfiguration.SELECTION_ALGORITHM, DPSelectionAlgorithm.class)
        .set(StaticMWOConfiguration.START_TIME, currTime)
        .build());
    operatorIds.add("PAFAS");


    // PAFAS-Greedy-incremental
    configurationList.add(IncrementMWOConfiguration.CONF
        .set(IncrementMWOConfiguration.INITIAL_TIMESCALES, "(4,2)(5,3)(6,4)(10,5)")
        .set(IncrementMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
        .set(IncrementMWOConfiguration.SELECTION_ALGORITHM, GreedySelectionAlgorithm.class)
        .set(IncrementMWOConfiguration.START_TIME, currTime)
        .build());
    operatorIds.add("PAFAS-INC");

*/

/*
    // single
    configurationList.add(StaticSingleMWOConfiguration.CONF
        .set(StaticSingleMWOConfiguration.INITIAL_TIMESCALES, "(10,4)(11,1)(12,3)(13,2)(15,5)(16,2)(17,5)(19,5)(20,2)(21,5)(22,2)(23,3)(24,2)(25,2)(26,1)(27,4)(28,1)(29,2)(30,4)(33,4)(34,4)(35,4)(36,5)(38,5)(40,3)(41,1)(43,3)(44,4)(45,5)(46,1)(48,4)(50,2)(53,1)(55,1)(56,5)(58,1)(59,3)(61,2)(62,1)(64,3)(65,2)(66,3)(67,2)(68,2)(70,3)(71,4)(72,3)(73,2)(75,4)(76,5)(78,3)(79,3)(80,3)(81,1)(82,2)(83,1)(84,3)(88,5)(91,1)(92,5)(93,3)(94,1)(95,4)(96,4)(97,1)(98,1)(99,4)(100,1)(101,1)(102,3)(103,1)(104,1)(106,4)(108,3)(109,3)(110,4)(111,1)(112,4)(114,3)(115,2)(116,4)(117,1)(119,1)(120,2)(121,4)(123,4)(125,5)(126,2)(127,5)(128,3)(130,4)(132,5)(133,3)(134,2)(135,5)(136,1)(137,3)(139,5)(140,1)(141,3)(142,1)(144,4)(145,2)(146,2)(147,4)(148,2)(149,2)(150,2)(152,1)(153,4)(155,5)(156,5)(157,5)(158,3)(159,5)(160,5)(161,5)(162,5)(163,3)(164,3)(166,4)(167,4)(170,4)(171,3)(172,1)(174,3)(175,3)(176,2)(177,5)(178,5)(179,2)(180,2)(181,2)(182,3)(183,5)(184,5)(186,2)(187,4)(188,2)(189,4)(190,5)(191,4)(192,1)(193,1)(194,5)(195,1)(196,4)(198,3)(199,2)(200,5)")
        .set(StaticSingleMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
        .set(StaticSingleMWOConfiguration.SELECTION_ALGORITHM, GreedySelectionAlgorithm.class)
        .set(StaticSingleMWOConfiguration.START_TIME, currTime)
        .build());
    operatorIds.add("PAFAS-SINGLE-GREEDY");
*/

    // single
    configurationList.add(StaticSingleMWOConfiguration.CONF
        .set(StaticSingleMWOConfiguration.INITIAL_TIMESCALES, "(10,1)(30,3)(60,6)(120,12)(300,30)(600,60)(900,90)(1200,120)(1500,150)(1800,180)")
        .set(StaticSingleMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
        .set(StaticSingleMWOConfiguration.SELECTION_ALGORITHM, DPSelectionAlgorithm.class)
        .set(StaticSingleMWOConfiguration.START_TIME, currTime)
        .build());
    operatorIds.add("PAFAS-SINGLE-DP");

    // Infinite
    /*
    configurationList.add(InfiniteMWOConfiguration.CONF
        .set(InfiniteMWOConfiguration.INITIAL_TIMESCALES, "(4,1)(5,2)(6,3)(10,4)")
        .set(InfiniteMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
        .set(InfiniteMWOConfiguration.SELECTION_ALGORITHM, DPSelectionAlgorithm.class)
        .set(InfiniteMWOConfiguration.START_TIME, currTime)
        .build());
    operatorIds.add("PAFAS-INF");
    */
/*
    // On-the-fly operator
    configurationList.add(OntheflyMWOConfiguration.CONF
        .set(OntheflyMWOConfiguration.INITIAL_TIMESCALES, "(4,2)(5,3)(6,4)")
        .set(OntheflyMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
        .set(OntheflyMWOConfiguration.START_TIME, currTime)
        .build());
    operatorIds.add("OntheFly");

    // TriOPs
    configurationList.add(TriOpsMWOConfiguration.CONF
        .set(TriOpsMWOConfiguration.INITIAL_TIMESCALES, "(4,2)(5,3)(6,4)")
        .set(TriOpsMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
        .set(TriOpsMWOConfiguration.START_TIME, currTime)
        .build());
    operatorIds.add("TriOps");
*/
    int i = 0;
    final List<TimescaleWindowOperator> mwos = new LinkedList<>();
    final List<AggregationCounter> aggregationCounters = new LinkedList<>();
    for (final Configuration conf : configurationList) {
      final Injector injector = Tang.Factory.getTang().newInjector(Configurations.merge(jcb.build(), conf));
      injector.bindVolatileInstance(TimeWindowOutputHandler.class, new LoggingHandler<>(operatorIds.get(i)));
      injector.bindVolatileParameter(EndTime.class, 500L);
      final TimescaleWindowOperator<Object, Map<String, Long>> mwo = injector.getInstance(PafasMWO.class);
      mwos.add(mwo);
      final AggregationCounter aggregationCounter = injector.getInstance(AggregationCounter.class);
      aggregationCounters.add(aggregationCounter);
      i += 1;
    }

    final int numKey = 1000;
    final int numInput = 10000;
    final Random random = new Random();
    long prevTickTime = System.nanoTime();
    long tick = 1;
    for (i = 0; i < numInput; i++) {
      final int key = Math.abs(random.nextInt()%numKey);
      long cTickTime = System.nanoTime();
      if (TimeUnit.NANOSECONDS.toMillis(cTickTime - prevTickTime) >= 1000) {
        for (final TimescaleWindowOperator mwo : mwos) {
          mwo.execute(new WindowTimeEvent(tick));
        }
        tick += 1;
        prevTickTime += TimeUnit.SECONDS.toNanos(1);
      } else {
        for (final TimescaleWindowOperator mwo : mwos) {
          mwo.execute(Integer.toString(key));
        }
      }
      for (final TimescaleWindowOperator mwo : mwos) {
        mwo.execute(Integer.toString(key));
      }
      Thread.sleep(10);
    }

    for (final TimescaleWindowOperator mwo : mwos) {
      mwo.close();
    }

    i = 0;
    for (final TimescaleWindowOperator mwo : mwos) {
      final AggregationCounter aggregationCounter = aggregationCounters.get(i);
      final long partialCount = aggregationCounter.getNumPartialAggregation();
      final long finalCount = aggregationCounter.getNumFinalAggregation();
      final String id = operatorIds.get(i);
      LOG.info(id + " aggregation count: partial: " + partialCount + ", final: " + finalCount
          + ", total: " + (partialCount + finalCount));
      i += 1;
    }
  }
}
