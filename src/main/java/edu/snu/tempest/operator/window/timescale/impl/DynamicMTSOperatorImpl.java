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
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;

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
  private final PartialAggregator<I, V> partialAggregator;

  /**
   * Output emitter.
   */
  private OutputEmitter<TimescaleWindowOutput<V>> emitter;

  /**
   * Overlapping window stage executing overlapping window operators.
   */
  private final FinalAggregatorManager<V> owoStage;

  /**
   * Overlapping window subscriptions.
   */
  private final Map<Timescale, Subscription<FinalAggregator<V>>> subscriptions;

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
   * Slice time provider which provides next slice time for partial result.
   */
  private final NextEdgeProvider sliceTimeProvider;

  private SlicingStage<I, V> slicingStage;

  /**
   * Creates dynamic MTS window operator.
   * @param slicingStage a slicing stage which triggers slice of partial result
   * @param owoStage an overlapping window stage which executes overlapping window operators
   * @param spanTracker a computation reuser which saves partial/final results
   * @param tsParser timescale parser for initial timescales
   * @param partialAggregator  a sliced window operator which creates partial result
   * @param sliceTimeProvider a slice time provider which provides next slice time
   * @param slicedWindowEmitter an output emitter which saves partial results and triggers final aggregation
   * @param startTime start time of this operator
   * @throws Exception
   */
  @Inject
  private DynamicMTSOperatorImpl(
      final FinalAggregatorManager<V> owoStage,
      final SpanTracker<V> spanTracker,
      final TimescaleParser tsParser,
      final PartialAggregator<I, V> partialAggregator,
      final NextEdgeProvider sliceTimeProvider,
      final PartialAggregatorOutputEmitter<V> slicedWindowEmitter,
      @Parameter(StartTime.class) final long startTime) throws Exception {
    this.owoStage = owoStage;
    this.partialAggregator = partialAggregator;
    this.sliceTimeProvider = sliceTimeProvider;
    this.spanTracker = spanTracker;
    this.partialAggregator.prepare(slicedWindowEmitter);
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
    this.partialAggregator.execute(val);
  }

  /**
   * Creates initial overlapping window operators.
   * @param outputEmitter an output emitter
   */
  @Override
  public void prepare(final OutputEmitter<TimescaleWindowOutput<V>> outputEmitter) {
    this.emitter = outputEmitter;
    final Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileInstance(PartialAggregator.class, partialAggregator);
    injector.bindVolatileParameter(StartTime.class, startTime);
    try {
      slicingStage = injector.getInstance(SlicingStage.class);
    } catch (InjectionException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    // add overlapping window operators
    for (final Timescale timescale : tsParser.timescales) {
      final FinalAggregator<V> owo = new DefaultFinalAggregator<>(
          timescale, spanTracker, startTime);
      owo.prepare(emitter);
      final Subscription<FinalAggregator<V>> ss = this.owoStage.subscribe(owo);
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
    //Make as if the new timescale is added at adjTime.
    final long adjTime = addTime - ((addTime - startTime) % timescale.intervalSize);

    //1. change slice time.
    this.sliceTimeProvider.onTimescaleAddition(timescale, adjTime);

    //2. add timescale to computationReuser.
    this.spanTracker.onTimescaleAddition(timescale, adjTime);

    //3. add overlapping window operator
    final FinalAggregator<V> owo = new DefaultFinalAggregator<>(
        timescale, spanTracker, adjTime);
    owo.prepare(emitter);
    final Subscription<FinalAggregator<V>> ss = this.owoStage.subscribe(owo);
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
    final Subscription<FinalAggregator<V>> ss = subscriptions.get(timescale);
    if (ss == null) {
      LOG.log(Level.WARNING, "Deletion error: Timescale " + timescale + " not exists. ");
    } else {
      this.sliceTimeProvider.onTimescaleDeletion(timescale, deleteTime);
      this.spanTracker.onTimescaleDeletion(timescale, deleteTime);
      ss.unsubscribe();
    }
  }

  @Override
  public void close() throws Exception {
    slicingStage.close();
    owoStage.close();
    spanTracker.close();
  }
}
