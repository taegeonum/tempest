package vldb.operator.window.timescale.pafas;

import org.apache.reef.tang.*;
import org.junit.Test;
import vldb.example.DefaultExtractor;
import vldb.operator.window.aggregator.impl.CountByKeyAggregator;
import vldb.operator.window.aggregator.impl.KeyExtractor;
import vldb.operator.window.timescale.TimeWindowOutputHandler;
import vldb.operator.window.timescale.TimescaleWindowOperator;
import vldb.operator.window.timescale.onthefly.OntheflyMWOConfiguration;
import vldb.operator.window.timescale.pafas.event.WindowTimeEvent;
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
public final class PafasMWOTest {
  private static final Logger LOG = Logger.getLogger(PafasMWOTest.class.getName());

  @Test
  public void testPafasMWO() throws Exception {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindImplementation(KeyExtractor.class, DefaultExtractor.class);
    jcb.bindNamedParameter(NumThreads.class, "4");

    final long currTime = 0;
    final List<Configuration> configurationList = new LinkedList<>();
    final List<String> operatorIds = new LinkedList<>();
    final String timescaleString =  "(4,2)(5,3)(6,4)(10,5)";

    // PAFAS
    configurationList.add(StaticSingleMWOConfiguration.CONF
        .set(StaticSingleMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
        .set(StaticSingleMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
        .set(StaticSingleMWOConfiguration.SELECTION_ALGORITHM, DPSelectionAlgorithm.class)
        .set(StaticSingleMWOConfiguration.OUTPUT_LOOKUP_TABLE, DPOutputLookupTableImpl.class)
        .set(StaticSingleMWOConfiguration.START_TIME, "0")
        .build());
    operatorIds.add("PAFAS");

    /*
    // PAFAS-Greedy
    configurationList.add(StaticMWOConfiguration.CONF
        .set(StaticMWOConfiguration.INITIAL_TIMESCALES, "(4,2)(5,3)(6,4)(10,5)")
        .set(StaticMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
        .set(StaticMWOConfiguration.SELECTION_ALGORITHM, DPSelectionAlgorithm.class)
        .set(StaticMWOConfiguration.START_TIME, currTime)
        .build());
    operatorIds.add("PAFAS-DP");
*/


    // On-the-fly operator
    configurationList.add(OntheflyMWOConfiguration.CONF
        .set(OntheflyMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
        .set(OntheflyMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
        .set(OntheflyMWOConfiguration.START_TIME, currTime)
        .build());
    operatorIds.add("OntheFly");

    /*
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
      final TimescaleWindowOperator<String, Map<String, Long>> mwo = injector.getInstance(PafasMWO.class);
      mwos.add(mwo);
      final AggregationCounter aggregationCounter = injector.getInstance(AggregationCounter.class);
      aggregationCounters.add(aggregationCounter);
      i += 1;
    }

    final int numKey = 10;
    final int numInput = 10000;
    final Random random = new Random();
    final int tick = numInput / 30;
    int tickTime = 1;
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
