package vldb.example;

import org.apache.reef.tang.*;
import org.apache.reef.tang.exceptions.InjectionException;
import vldb.operator.OutputEmitter;
import vldb.operator.window.aggregator.impl.CountByKeyAggregator;
import vldb.operator.window.aggregator.impl.KeyExtractor;
import vldb.operator.window.timescale.TimeWindowOutputHandler;
import vldb.operator.window.timescale.TimescaleWindowOperator;
import vldb.operator.window.timescale.TimescaleWindowOutput;
import vldb.operator.window.timescale.pafas.GreedySelectionAlgorithm;
import vldb.operator.window.timescale.pafas.PafasMWO;
import vldb.operator.window.timescale.pafas.StaticMWOConfiguration;
import vldb.operator.window.timescale.parameter.NumThreads;

import java.util.Map;

public final class PafasOperatorExample {

  public static void main(final String[] args) throws InjectionException, InterruptedException {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindImplementation(KeyExtractor.class, DefaultExtractor.class);
    jcb.bindNamedParameter(NumThreads.class, "4");

    final Configuration pafasConf = StaticMWOConfiguration.CONF
        .set(StaticMWOConfiguration.INITIAL_TIMESCALES, "(4,2)(5,3)(6,4)(10,5)")
        .set(StaticMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
        .set(StaticMWOConfiguration.SELECTION, GreedySelectionAlgorithm.class)
        .build();

    final Injector injector = Tang.Factory.getTang().newInjector(
        Configurations.merge(jcb.build(), pafasConf));
    injector.bindVolatileInstance(TimeWindowOutputHandler.class, new TimeWindowOutputHandler<Map<String, Long>, Long>() {
      @Override
      public void execute(final TimescaleWindowOutput<Map<String, Long>> val) {
        System.out.println("ts: " + val.timescale +
            ", timespan: [" + val.startTime + ", " + val.endTime + ")"
        + ", output: " + val.output.result);
      }

      @Override
      public void prepare(final OutputEmitter<TimescaleWindowOutput<Long>> outputEmitter) {

      }
    });

    final TimescaleWindowOperator<String, Map<String, Long>> operator = injector.getInstance(PafasMWO.class);
    for (int i = 0; i < 10000; i++) {
      operator.execute(Integer.toString(i%5));
      Thread.sleep(10);
    }
  }
}
