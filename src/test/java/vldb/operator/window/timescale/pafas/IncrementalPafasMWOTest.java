package vldb.operator.window.timescale.pafas;

import org.apache.reef.tang.*;
import org.junit.Test;
import vldb.evaluation.parameter.EndTime;
import vldb.example.DefaultExtractor;
import vldb.operator.window.aggregator.impl.CountByKeyAggregator;
import vldb.operator.window.aggregator.impl.KeyExtractor;
import vldb.operator.window.timescale.TimeWindowOutputHandler;
import vldb.operator.window.timescale.TimescaleWindowOperator;
import vldb.operator.window.timescale.parameter.NumThreads;
import vldb.operator.window.timescale.profiler.AggregationCounter;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
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

    */
    // PAFAS-Greedy-incremental
    configurationList.add(IncrementMWOConfiguration.CONF
        .set(IncrementMWOConfiguration.INITIAL_TIMESCALES, "(4,2)(5,3)(6,4)(10,5)")
        .set(IncrementMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
        .set(IncrementMWOConfiguration.SELECTION_ALGORITHM, DPSelectionAlgorithm.class)
        .set(IncrementMWOConfiguration.START_TIME, currTime)
        .build());
    operatorIds.add("PAFAS-INC");


    // single
    configurationList.add(StaticSingleMWOConfiguration.CONF
        .set(StaticSingleMWOConfiguration.INITIAL_TIMESCALES, "(4,2)(5,3)(6,4)(10,5)")
        .set(StaticSingleMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
        .set(StaticSingleMWOConfiguration.SELECTION_ALGORITHM, DPSelectionAlgorithm.class)
        .set(StaticSingleMWOConfiguration.START_TIME, currTime)
        .build());
    operatorIds.add("PAFAS-SINGLE");


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
      final TimescaleWindowOperator<String, Map<String, Long>> mwo = injector.getInstance(PafasMWO.class);
      mwos.add(mwo);
      final AggregationCounter aggregationCounter = injector.getInstance(AggregationCounter.class);
      aggregationCounters.add(aggregationCounter);
      i += 1;
    }

    final int numKey = 10;
    final int numInput = 5000;
    final Random random = new Random();
    for (i = 0; i < numInput; i++) {
      final int key = Math.abs(random.nextInt()%numKey);
      for (final TimescaleWindowOperator mwo : mwos) {
        mwo.execute(Integer.toString(key));
      }
      Thread.sleep(5);
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
