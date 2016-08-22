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
import vldb.operator.window.timescale.pafas.dynamic.*;
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
        .set(DynamicMWOConfiguration.INITIAL_TIMESCALES, "(1800,180)")
        .set(DynamicMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
        .set(DynamicMWOConfiguration.SELECTION_ALGORITHM, DynamicDPSelectionAlgorithm.class)
        .set(DynamicMWOConfiguration.OUTPUT_LOOKUP_TABLE, DynamicDPOutputLookupTableImpl.class)
        .set(DynamicMWOConfiguration.START_TIME, currTime)
        .build();

    final Injector injector = Tang.Factory.getTang().newInjector(Configurations.merge(jcb.build(), conf));
    injector.bindVolatileInstance(TimeWindowOutputHandler.class, new LoggingHandler<>("DynamicMWO"));
    injector.bindVolatileParameter(EndTime.class, 500L);
    final DynamicMWO<Object, Map<String, Long>> mwo = injector.getInstance(DynamicMWO.class);
    final AggregationCounter aggregationCounter = injector.getInstance(AggregationCounter.class);

    final List<Timescale> timescaleList = TimescaleParser.parseFromStringNoSort("(1800,180)(1500,150)(1200,120)(900,90)(600,60)(300,30)(180,18)(120,12)(60,6)(30,3)");
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
        mwo.execute(new WindowTimeEvent(tick));


        // Add window
        if (tick <= 90 && tick % 10 == 0) {
          final int index = (int)tick / 10;
          System.out.println("ADD WIndow " + tick + ", " + timescaleList.get(index));
          mwo.addWindow(timescaleList.get(index), tick);
        }


        if (tick > 90 && tick <= 180 && tick % 10 == 0) {
          int index = 9 - ((int)(tick/10)%10);
          final Timescale ts = timescaleList.get(index);
          System.out.println("RM WIndow " + tick + ", " + ts);
          final long rmStartTime = System.nanoTime();
          mwo.removeWindow(ts, tick);
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
