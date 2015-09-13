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
import edu.snu.tempest.operator.common.Subscription;
import edu.snu.tempest.operator.window.timescale.*;
import edu.snu.tempest.operator.window.timescale.parameter.StartTime;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Dynamic Multi-time scale sliding window operator.
 * This operator can dynamically add or remove timescales.
 * @param <I> input
 * @param <V> output
 */
public final class DynamicMTSOperatorImpl<I, V> implements DynamicMTSWindowOperator<I, V> {
  private static final Logger LOG = Logger.getLogger(DynamicMTSOperatorImpl.class.getName());

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
   * Overlapping window subscriptions.
   */
  private final Map<Timescale, Subscription<OverlappingWindowOperator<V>>> subscriptions;

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

  /**
   * Slice time provider which provides next slice time for partial result.
   */
  private final NextSliceTimeProvider sliceTimeProvider;

  /**
   * Creates dynamic MTS window operator.
   * @param slicingStage a slicing stage which triggers slice of partial result
   * @param owoStage an overlapping window stage which executes overlapping window operators
   * @param computationReuser a computation reuser which saves partial/final results
   * @param tsParser timescale parser for initial timescales
   * @param slicedWindowOperator  a sliced window operator which creates partial result
   * @param sliceTimeProvider a slice time provider which provides next slice time
   * @param slicedWindowEmitter an output emitter which saves partial results and triggers final aggregation
   * @param startTime start time of this operator
   * @throws Exception
   */
  @Inject
  private DynamicMTSOperatorImpl(
      final SlicingStage<I, V> slicingStage,
      final OverlappingWindowStage<V> owoStage,
      final ComputationReuser<V> computationReuser,
      final TimescaleParser tsParser,
      final SlicedWindowOperator<I, V> slicedWindowOperator,
      final NextSliceTimeProvider sliceTimeProvider,
      final SlicedWindowOperatorOutputEmitter<V> slicedWindowEmitter,
      @Parameter(StartTime.class) final long startTime) throws Exception {
    this.owoStage = owoStage;
    this.slicedWindowOperator = slicedWindowOperator;
    this.sliceTimeProvider = sliceTimeProvider;
    this.computationReuser = computationReuser;
    this.slicedWindowOperator.prepare(slicedWindowEmitter);
    this.subscriptions = new HashMap<>();
    this.tsParser = tsParser;
    this.startTime = startTime;
  }

  /**
   * Aggregate input and produce partially aggregated outputs.
   * @param val input value
   */
  @Override
  public void execute(final I val) {
    LOG.log(Level.FINEST, DynamicMTSOperatorImpl.class.getName() + " execute : ( " + val + ")");
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
    for (final Timescale timescale : tsParser.timescales) {
      final OverlappingWindowOperator<V> owo = new DefaultOverlappingWindowOperator<>(
          timescale, computationReuser, startTime);
      owo.prepare(emitter);
      final Subscription<OverlappingWindowOperator<V>> ss = this.owoStage.subscribe(owo);
      subscriptions.put(ss.getToken().getTimescale(), ss);
    }
  }

  /**
   * Dynamically add a timescale.
   * @param timescale timescale
   * @param addTime the time when timescale is added. TimeUnit is second.
   */
  @Override
  public void onTimescaleAddition(final Timescale timescale, final long addTime) {
    LOG.log(Level.INFO, DynamicMTSOperatorImpl.class.getName() + " addTimescale: " + timescale);
    // adjust add time

    //1. change slice time.
    this.sliceTimeProvider.onTimescaleAddition(timescale, addTime);

    //2. add timescale to computationReuser.
    this.computationReuser.onTimescaleAddition(timescale, addTime);

    //3. add overlapping window operator
    final OverlappingWindowOperator<V> owo = new DefaultOverlappingWindowOperator<>(
        timescale, computationReuser, addTime);
    owo.prepare(emitter);
    final Subscription<OverlappingWindowOperator<V>> ss = this.owoStage.subscribe(owo);
    subscriptions.put(ss.getToken().getTimescale(), ss);
  }

  /**
   * Dynamically remove a timescale.
   * @param timescale
   * @param deleteTime the time when timescale is deleted. TimeUnit is second.
   */
  @Override
  public void onTimescaleDeletion(final Timescale timescale, final long deleteTime) {
    LOG.log(Level.INFO, DynamicMTSOperatorImpl.class.getName() + " removeTimescale: " + timescale);
    final Subscription<OverlappingWindowOperator<V>> ss = subscriptions.get(timescale);
    if (ss == null) {
      LOG.log(Level.WARNING, "Deletion error: Timescale " + timescale + " not exists. ");
    } else {
      this.sliceTimeProvider.onTimescaleDeletion(timescale, deleteTime);
      this.computationReuser.onTimescaleDeletion(timescale, deleteTime);
      ss.unsubscribe();
    }
  }
}
