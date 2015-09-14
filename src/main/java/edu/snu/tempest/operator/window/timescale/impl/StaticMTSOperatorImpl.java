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
package edu.snu.tempest.operator.window.timescale.impl;

import edu.snu.tempest.operator.OutputEmitter;
import edu.snu.tempest.operator.window.timescale.Timescale;
import edu.snu.tempest.operator.window.timescale.TimescaleWindowOperator;
import edu.snu.tempest.operator.window.timescale.TimescaleWindowOutput;
import edu.snu.tempest.operator.window.timescale.parameter.StartTime;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Static Multi-time scale sliding window operator.
 * @param <I> input
 * @param <V> output
 */
public final class StaticMTSOperatorImpl<I, V> implements TimescaleWindowOperator<I, V> {
  private static final Logger LOG = Logger.getLogger(StaticMTSOperatorImpl.class.getName());

  /**
   * A sliced window operator for incremental aggregation.
   */
  private final SlicedWindowOperator<I, V> slicedWindowOperator;

  /**
   * Output emitter.
   */
  private OutputEmitter<TimescaleWindowOutput<V>> emitter;

  /**
   * Overlapping window stage executing overlapping window operators.
   */
  private final OverlappingWindowStage<V> owoStage;

  /**
   * Parser for Initial timescales.
   */
  private final TimescaleParser tsParser;

  /**
   * Computation reuser whichc saves partial/final results
   * in order to do computation reuse between multiple timescales.
   */
  private final ComputationReuser<V> computationReuser;

  /**
   * Start time of this operator.
   */
  private final long startTime;

  private SlicingStage<I, V> slicingStage;

  /**
   * Creates static MTS window operator.
   * @param slicingStage a slicing stage which triggers slice of partial result
   * @param owoStage an overlapping window stage which executes overlapping window operators
   * @param computationReuser a computation reuser which saves partial/final results
   * @param tsParser timescale parser for initial timescales
   * @param slicedWindowOperator  a sliced window operator which creates partial result
   * @param slicedWindowEmitter an output emitter which saves partial results and triggers final aggregation
   * @param startTime start time of this operator
   * @throws Exception
   */
  @Inject
  private StaticMTSOperatorImpl(
      final OverlappingWindowStage<V> owoStage,
      final ComputationReuser<V> computationReuser,
      final TimescaleParser tsParser,
      final SlicedWindowOperator<I, V> slicedWindowOperator,
      final SlicedWindowOperatorOutputEmitter<V> slicedWindowEmitter,
      @Parameter(StartTime.class) final long startTime) throws Exception {
    this.slicedWindowOperator = slicedWindowOperator;
    this.computationReuser = computationReuser;
    this.slicedWindowOperator.prepare(slicedWindowEmitter);
    this.tsParser = tsParser;
    this.startTime = startTime;
    this.owoStage = owoStage;
  }

  /**
   * Aggregate input and produce partially aggregated outputs.
   * @param val input value
   */
  @Override
  public void execute(final I val) {
    LOG.log(Level.FINEST, StaticMTSOperatorImpl.class.getName() + " execute : ( " + val + ")");
    this.slicedWindowOperator.execute(val);
  }

  /**
   * Creates initial overlapping window operators.
   * @param outputEmitter an output emitter
   */
  @Override
  public void prepare(final OutputEmitter<TimescaleWindowOutput<V>> outputEmitter) {
    this.emitter = outputEmitter;
    // add overlapping window operators
    final Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileInstance(SlicedWindowOperator.class, slicedWindowOperator);
    injector.bindVolatileParameter(StartTime.class, startTime);
    try {
      slicingStage = injector.getInstance(SlicingStage.class);
    } catch (InjectionException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    for (final Timescale timescale : tsParser.timescales) {
      final OverlappingWindowOperator<V> owo = new DefaultOverlappingWindowOperator<>(
          timescale, computationReuser, startTime);
      owo.prepare(emitter);
      this.owoStage.subscribe(owo);
    }
  }

  @Override
  public void close() throws Exception {
    slicingStage.close();
    owoStage.close();
  }
}