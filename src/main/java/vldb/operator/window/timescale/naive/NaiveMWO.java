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
import vldb.operator.OutputEmitter;
import vldb.operator.window.aggregator.impl.CountByKeyAggregator;
import vldb.operator.window.timescale.Timescale;
import vldb.operator.window.timescale.TimescaleWindowOperator;
import vldb.operator.window.timescale.TimescaleWindowOutput;
import vldb.operator.window.timescale.common.TimescaleParser;
import vldb.operator.window.timescale.pafas.GreedySelectionAlgorithm;
import vldb.operator.window.timescale.pafas.PafasMWO;
import vldb.operator.window.timescale.pafas.StaticMWOConfiguration;
import vldb.operator.window.timescale.parameter.StartTime;
import vldb.operator.window.timescale.profiler.AggregationCounter;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

/**
 * Static Multi-time scale sliding window operator.
 * @param <I> input
 * @param <V> output
 */
public final class NaiveMWO<I, V> implements TimescaleWindowOperator<I, V> {
  private static final Logger LOG = Logger.getLogger(NaiveMWO.class.getName());

  private final List<PafasMWO<I, V>> operators;
  /**
   * Creates Pafas MWO.
   * @throws Exception
   */
  @Inject
  private NaiveMWO(
      final TimescaleParser tsParser,
      final AggregationCounter aggregationCounter,
      @Parameter(StartTime.class) final long startTime) throws Exception {
    this.operators = new LinkedList<>();
    final List<Timescale> timescales = tsParser.timescales;
    for (final Timescale timescale : timescales) {
      final Configuration conf = StaticMWOConfiguration.CONF
          .set(StaticMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
          .set(StaticMWOConfiguration.SELECTION_ALGORITHM, GreedySelectionAlgorithm.class)
          .set(StaticMWOConfiguration.START_TIME, startTime+"")
          .set(StaticMWOConfiguration.INITIAL_TIMESCALES, timescale.toString())
          .build();
      final Injector injector = Tang.Factory.getTang().newInjector(conf);
      injector.bindVolatileInstance(AggregationCounter.class, aggregationCounter);
      final PafasMWO<I, V> operator = injector.getInstance(PafasMWO.class);
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
}