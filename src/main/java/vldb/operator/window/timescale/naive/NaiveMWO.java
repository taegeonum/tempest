/*
 * Copyright (C) 2015 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package vldb.operator.window.timescale.naive;

import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;
import vldb.evaluation.Metrics;
import vldb.evaluation.parameter.EndTime;
import vldb.operator.Operator;
import vldb.operator.OutputEmitter;
import vldb.operator.window.aggregator.impl.CountByKeyAggregator;
import vldb.operator.window.aggregator.impl.KeyExtractor;
import vldb.operator.window.timescale.*;
import vldb.operator.window.timescale.common.DefaultOutputLookupTableImpl;
import vldb.operator.window.timescale.common.TimescaleParser;
import vldb.operator.window.timescale.pafas.PafasMWO;
import vldb.operator.window.timescale.parameter.StartTime;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Static Multi-time scale sliding window operator.
 * @param <I> input
 * @param <V> output
 */
public final class NaiveMWO<I, V> implements TimescaleWindowOperator<I, V> {
  private static final Logger LOG = Logger.getLogger(NaiveMWO.class.getName());

  private final List<PafasMWO<I, V>> operators;

  private final Metrics metrics;

  private final KeyExtractor keyExtractor;

  private final TimeWindowOutputHandler timeWindowOutputHandler;

  private final TimeMonitor timeMonitor;

  private final long endTime;

  private final Map<Timescale, Operator> timescaleOperatorMap = new HashMap<>();
  /**
   * Creates Pafas MWO.
   * @throws Exception
   */
  @Inject
  private NaiveMWO(
      final TimescaleParser tsParser,
      final Metrics metrics,
      final KeyExtractor keyExtractor,
      final TimeWindowOutputHandler timeWindowOutputHandler,
      final TimeMonitor timeMonitor,
      @Parameter(StartTime.class) final long startTime,
      @Parameter(EndTime.class) final long endTime) throws Exception {
    this.operators = new LinkedList<>();
    this.timeMonitor = timeMonitor;
    this.metrics = metrics;
    this.keyExtractor = keyExtractor;
    this.timeWindowOutputHandler = timeWindowOutputHandler;
    this.endTime = endTime;
    final List<Timescale> timescales = tsParser.timescales;
    for (final Timescale timescale : timescales) {
      final Configuration conf = StaticNaiveMWOConfiguration.CONF
          .set(StaticNaiveMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
          .set(StaticNaiveMWOConfiguration.START_TIME, startTime+"")
          .set(StaticNaiveMWOConfiguration.OUTPUT_LOOKUP_TABLE, DefaultOutputLookupTableImpl.class)
          .set(StaticNaiveMWOConfiguration.INITIAL_TIMESCALES, timescale.toString())
      .build();
      final Injector injector = Tang.Factory.getTang().newInjector(conf);
      injector.bindVolatileInstance(Metrics.class, metrics);
      injector.bindVolatileInstance(KeyExtractor.class, keyExtractor);
      injector.bindVolatileInstance(TimeWindowOutputHandler.class, timeWindowOutputHandler);
      injector.bindVolatileParameter(EndTime.class, endTime);
      injector.bindVolatileInstance(TimeMonitor.class, timeMonitor);
      //injector.bindVolatileInstance(SharedForkJoinPool.class, sharedForkJoinPool);
      final PafasMWO<I, V> operator = injector.getInstance(PafasMWO.class);
      timescaleOperatorMap.put(timescale, operator);
      operators.add(operator);
    }
  }

  /**
   * Aggregate input and produce partially aggregated outputs.
   * @param val input value
   */
  @Override
  public void execute(final I val) {
    //LOG.log(Level.FINEST, PafasMWO.class.getName() + " execute : ( " + val + ")");
    for (final PafasMWO<I, V> operator : operators) {
      operator.execute(val);
    }
  }

  /**
   * Creates initial overlapping window operators.
   * @param outputEmitter an output emitter
   */
  @Override
  public void prepare(final OutputEmitter<TimescaleWindowOutput<V>> outputEmitter) {

  }

  @Override
  public void close() throws Exception {
    for (final PafasMWO<I, V> operator : operators) {
      operator.close();
    }
  }

  @Override
  public void addWindow(final Timescale ts, final long time) {
      final Configuration conf = StaticNaiveMWOConfiguration.CONF
          .set(StaticNaiveMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
          .set(StaticNaiveMWOConfiguration.START_TIME, time+"")
          .set(StaticNaiveMWOConfiguration.OUTPUT_LOOKUP_TABLE, DefaultOutputLookupTableImpl.class)
          .set(StaticNaiveMWOConfiguration.INITIAL_TIMESCALES, ts.toString())
          .build();
      final Injector injector = Tang.Factory.getTang().newInjector(conf);
      injector.bindVolatileInstance(Metrics.class, metrics);
      injector.bindVolatileInstance(KeyExtractor.class, keyExtractor);
      injector.bindVolatileInstance(TimeWindowOutputHandler.class, timeWindowOutputHandler);
      injector.bindVolatileParameter(EndTime.class, endTime);
      injector.bindVolatileInstance(TimeMonitor.class, timeMonitor);
      //injector.bindVolatileInstance(SharedForkJoinPool.class, sharedForkJoinPool);
    try {
      final PafasMWO<I, V> operator = injector.getInstance(PafasMWO.class);
      operators.add(operator);
      timescaleOperatorMap.put(ts, operator);
    } catch (InjectionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void removeWindow(final Timescale ts, final long time) {
    final Operator operator = timescaleOperatorMap.remove(ts);
    operators.remove(operator);
  }
}