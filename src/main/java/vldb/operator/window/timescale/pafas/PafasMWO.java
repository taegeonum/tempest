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
package vldb.operator.window.timescale.pafas;

import org.apache.reef.tang.annotations.Parameter;
import vldb.operator.OutputEmitter;
import vldb.operator.window.timescale.Timescale;
import vldb.operator.window.timescale.TimescaleWindowOperator;
import vldb.operator.window.timescale.TimescaleWindowOutput;
import vldb.operator.window.timescale.common.FinalAggregator;
import vldb.operator.window.timescale.common.PartialAggregator;
import vldb.operator.window.timescale.common.SpanTracker;
import vldb.operator.window.timescale.common.TimescaleParser;
import vldb.operator.window.timescale.parameter.StartTime;

import javax.inject.Inject;
import java.util.logging.Logger;

/**
 * Static Multi-time scale sliding window operator.
 * @param <I> input
 * @param <V> output
 */
public final class PafasMWO<I, V> implements TimescaleWindowOperator<I, V> {
  private static final Logger LOG = Logger.getLogger(PafasMWO.class.getName());

  /**
   * A sliced window operator for incremental aggregation.
   */
  private final PartialAggregator<I> partialAggregator;

  private final FinalAggregator<V> finalAggregator;

  /**
   * Parser for Initial timescales.
   */
  private final TimescaleParser tsParser;

  /**
   * Computation reuser whichc saves partial/final results
   * in order to do computation reuse between multiple timescales.
   */
  private final SpanTracker<V> spanTracker;

  /**
   * Start time of this operator.
   */
  private final long startTime;

  /**
   * Creates Pafas MWO.
   * @throws Exception
   */
  @Inject
  private PafasMWO(
      final SpanTracker<V> spanTracker,
      final TimescaleParser tsParser,
      final PartialAggregator<I> partialAggregator,
      final FinalAggregator<V> finalAggregator,
      @Parameter(StartTime.class) final long startTime) throws Exception {
    this.partialAggregator = partialAggregator;
    this.spanTracker = spanTracker;
    this.tsParser = tsParser;
    this.startTime = startTime;
    this.finalAggregator = finalAggregator;
  }

  /**
   * Aggregate input and produce partially aggregated outputs.
   * @param val input value
   */
  @Override
  public void execute(final I val) {
    this.partialAggregator.execute(val);
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
    partialAggregator.close();
    finalAggregator.close();
  }

  @Override
  public void addWindow(final Timescale ts, final long time) {
    throw new RuntimeException("No addWindow");
  }

  @Override
  public void removeWindow(final Timescale ts, final long time) {
    throw new RuntimeException("No removeWindow");
  }
}