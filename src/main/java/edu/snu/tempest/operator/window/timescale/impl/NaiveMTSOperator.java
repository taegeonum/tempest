package edu.snu.tempest.operator.window.timescale.impl;

import edu.snu.tempest.operator.OutputEmitter;
import edu.snu.tempest.operator.window.aggregator.CAAggregator;
import edu.snu.tempest.operator.window.timescale.Timescale;
import edu.snu.tempest.operator.window.timescale.TimescaleWindowOperator;
import edu.snu.tempest.operator.window.timescale.TimescaleWindowOutput;
import edu.snu.tempest.operator.window.timescale.parameter.StartTime;
import edu.snu.tempest.operator.window.timescale.parameter.TimescaleString;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by taegeonum on 9/13/15.
 */
public final class NaiveMTSOperator<I, V> implements TimescaleWindowOperator<I, V> {

  private final TimescaleParser tsParser;

  private final long startTime;

  private List<TimescaleWindowOperator<I, V>> operators;
  private final List<ExecutorService> executors;

  private OutputEmitter<TimescaleWindowOutput<V>> emitter;

  private final CAAggregator<I, V> aggregator;

  @Inject
  private NaiveMTSOperator(final TimescaleParser tsParser,
                           @Parameter(StartTime.class) final long startTime,
                           final CAAggregator<I, V> aggregator) throws Exception {
    this.tsParser = tsParser;
    this.startTime = startTime;
    this.operators = new LinkedList<>();
    this.aggregator = aggregator;
    this.executors = new LinkedList<>();
  }

  /**
   * Aggregate input and produce partially aggregated outputs.
   * @param val input value
   */
  @Override
  public void execute(final I val) {
    int i = 0;
    for (final TimescaleWindowOperator<I, V> operator : operators) {
      executors.get(i).submit(new Runnable() {
        @Override
        public void run() {
          operator.execute(val);
        }
      });
      i++;
    }
  }

  /**
   * Creates initial overlapping window operators.
   * @param outputEmitter an output emitter
   */
  @Override
  public void prepare(final OutputEmitter<TimescaleWindowOutput<V>> outputEmitter) {
    this.emitter = outputEmitter;
    for (final Timescale ts : tsParser.timescales) {
      final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
      final List<Timescale> tsList = new LinkedList<>();
      tsList.add(ts);
      jcb.bindNamedParameter(TimescaleString.class, TimescaleParser.parseToString(tsList));
      jcb.bindImplementation(ComputationReuser.class, StaticComputationReuser.class);
      jcb.bindImplementation(NextSliceTimeProvider.class, StaticNextSliceTimeProvider.class);
      jcb.bindImplementation(TimescaleWindowOperator.class, StaticMTSOperatorImpl.class);
      jcb.bindNamedParameter(StartTime.class, startTime+"");

      final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
      injector.bindVolatileInstance(CAAggregator.class, aggregator);
      try {
        final TimescaleWindowOperator<I, V> operator = injector.getInstance(TimescaleWindowOperator.class);
        operators.add(operator);
        operator.prepare(outputEmitter);
        executors.add(Executors.newFixedThreadPool(1));
      } catch (InjectionException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void close() throws Exception {
    int i = 0;
    for (final TimescaleWindowOperator<I, V> operator : operators) {
      operator.close();
      executors.get(i).shutdown();
      i++;
    }
  }
}
