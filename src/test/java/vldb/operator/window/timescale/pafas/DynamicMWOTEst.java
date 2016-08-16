package vldb.operator.window.timescale.pafas;

import org.apache.reef.tang.*;
import org.junit.Test;
import vldb.evaluation.parameter.EndTime;
import vldb.example.DefaultExtractor;
import vldb.operator.window.aggregator.impl.CountByKeyAggregator;
import vldb.operator.window.aggregator.impl.KeyExtractor;
import vldb.operator.window.timescale.TimeWindowOutputHandler;
import vldb.operator.window.timescale.Timescale;
import vldb.operator.window.timescale.common.TimescaleParser;
import vldb.operator.window.timescale.pafas.dynamic.DynamicDPSelectionAlgorithm;
import vldb.operator.window.timescale.pafas.dynamic.DynamicMWO;
import vldb.operator.window.timescale.pafas.dynamic.DynamicMWOConfiguration;
import vldb.operator.window.timescale.pafas.event.WindowTimeEvent;
import vldb.operator.window.timescale.parameter.NumThreads;
import vldb.operator.window.timescale.profiler.AggregationCounter;

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

    final Configuration conf = DynamicMWOConfiguration.CONF
        .set(DynamicMWOConfiguration.INITIAL_TIMESCALES, "(30,3)")
        .set(DynamicMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
        .set(DynamicMWOConfiguration.SELECTION_ALGORITHM, DynamicDPSelectionAlgorithm.class)
        .set(DynamicMWOConfiguration.START_TIME, currTime)
        .build();

    final Injector injector = Tang.Factory.getTang().newInjector(Configurations.merge(jcb.build(), conf));
    injector.bindVolatileInstance(TimeWindowOutputHandler.class, new LoggingHandler<>("DynamicMWO"));
    injector.bindVolatileParameter(EndTime.class, 500L);
    final DynamicMWO<Object, Map<String, Long>> mwo = injector.getInstance(DynamicMWO.class);
    final AggregationCounter aggregationCounter = injector.getInstance(AggregationCounter.class);

    final List<Timescale> timescaleList = TimescaleParser.parseFromString("(30,3)(60,6)(120,12)(180,18)(300,30)(600,60)(900,90)(1200,120)(1500,150)(1800,180)");
    final int numKey = 1000;
    final int numInput = 30000;
    final Random random = new Random();
    long tick = 1;
    for (int i = 1; i <= numInput; i++) {
      final int key = Math.abs(random.nextInt()%numKey);
      long cTickTime = System.nanoTime();
      if (i % 100 == 0) {
        System.out.println("Tick " + tick);
        mwo.execute(new WindowTimeEvent(tick));

        // Add window
        System.out.println("ADD WIndow " + tick);
        if (tick <= 9) {
          mwo.addWindow(timescaleList.get((int)tick), tick);
        }
        tick += 1;
      } else {
        mwo.execute(Integer.toString(key));
      }
      Thread.sleep(1);
    }

    mwo.close();
  }
}
