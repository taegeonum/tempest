package vldb.operator.window.timescale.pafas;

import org.apache.reef.tang.*;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Test;
import vldb.example.DefaultExtractor;
import vldb.operator.window.aggregator.impl.CountByKeyAggregator;
import vldb.operator.window.aggregator.impl.KeyExtractor;
import vldb.operator.window.timescale.TimeWindowOutputHandler;
import vldb.operator.window.timescale.TimescaleWindowOperator;
import vldb.operator.window.timescale.parameter.NumThreads;
import vldb.operator.window.timescale.triops.TriOpsMWOConfiguration;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * Created by taegeonum on 4/22/16.
 */
public final class PafasMWOTest {

  @Test
  public void testPafasMWO() throws InjectionException, InterruptedException {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindImplementation(KeyExtractor.class, DefaultExtractor.class);
    jcb.bindNamedParameter(NumThreads.class, "4");

    final long currTime = 0;

    // PAFAS-Greedy
    final Configuration pafasGreedyConf = StaticMWOConfiguration.CONF
        .set(StaticMWOConfiguration.INITIAL_TIMESCALES, "(4,2)(5,3)(6,4)")
        .set(StaticMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
        .set(StaticMWOConfiguration.SELECTION, GreedySelectionAlgorithm.class)
        .set(StaticMWOConfiguration.START_TIME, currTime)
        .build();


    final CountDownLatch countDownLatch1 = new CountDownLatch(30);
    final Injector injector = Tang.Factory.getTang().newInjector(Configurations.merge(jcb.build(), pafasGreedyConf));
    injector.bindVolatileInstance(TimeWindowOutputHandler.class,
        new LoggingHandler<>("PAFAS", countDownLatch1));
    final TimescaleWindowOperator<String, Map<String, Long>> pafasMWO = injector.getInstance(PafasMWO.class);


    // On-the-fly operator
    final Configuration ontheflyConf = StaticMWOConfiguration.CONF
        .set(StaticMWOConfiguration.INITIAL_TIMESCALES, "(4,2)(5,3)(6,4)")
        .set(StaticMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
        .set(StaticMWOConfiguration.SELECTION, OntheflySelectionAlgorithm.class)
        .set(StaticMWOConfiguration.START_TIME, currTime)
        .build();

    final CountDownLatch countDownLatch2 = new CountDownLatch(30);
    final Injector injector2 = Tang.Factory.getTang().newInjector(Configurations.merge(jcb.build(), ontheflyConf));
    injector2.bindVolatileInstance(TimeWindowOutputHandler.class,
        new LoggingHandler<>("OnTheFly", countDownLatch2));
    final TimescaleWindowOperator<String, Map<String, Long>> ontheflyMWO = injector2.getInstance(PafasMWO.class);

    // TriOPs
    final Configuration triOpsConf = TriOpsMWOConfiguration.CONF
        .set(StaticMWOConfiguration.INITIAL_TIMESCALES, "(4,2)(5,3)(6,4)")
        .set(StaticMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
        .set(StaticMWOConfiguration.SELECTION, OntheflySelectionAlgorithm.class)
        .set(StaticMWOConfiguration.START_TIME, currTime)
        .build();

    final CountDownLatch countDownLatch3 = new CountDownLatch(30);
    final Injector injector3 = Tang.Factory.getTang().newInjector(Configurations.merge(jcb.build(), triOpsConf));
    injector3.bindVolatileInstance(TimeWindowOutputHandler.class,
        new LoggingHandler<>("TriOPs", countDownLatch3));
    final TimescaleWindowOperator<String, Map<String, Long>> triOpsMWO = injector3.getInstance(PafasMWO.class);

    for (int i = 0; i < 3000; i++) {
      pafasMWO.execute(Integer.toString(i%5));
      ontheflyMWO.execute(Integer.toString(i%5));
      triOpsMWO.execute(Integer.toString(i%5));
      Thread.sleep(10);
    }
  }
}
