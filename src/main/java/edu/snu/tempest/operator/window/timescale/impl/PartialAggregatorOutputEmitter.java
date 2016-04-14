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

import javax.inject.Inject;

/**
 * This class saves partial output into computation reuser
 * and triggers overlapping window operators in order to do final aggregation.
 * @param <V> partial output
 */
final class PartialAggregatorOutputEmitter<V> implements OutputEmitter<PartialTimeWindowOutput<V>> {

  /**
   * Computation reuser whichc saves partial/final results
   * in order to do computation reuse between multiple timescales.
   */
  private final SpanTracker<V> spanTracker;

  /**
   * Overlapping window stage executing overlapping window operators.
   */
  private final FinalAggregatorManager<V> owoStage;

  /**
   * SlicedWindowOperatorOutputEmitter.
   * @param spanTracker a computation reuser
   * @param owoStage an overlapping window stage
   */
  @Inject
  private PartialAggregatorOutputEmitter(final SpanTracker<V> spanTracker,
                                         final FinalAggregatorManager<V> owoStage) {
    this.spanTracker = spanTracker;
    this.owoStage = owoStage;
  }

  /**
   * Save partial output into computation reuser
   * and trigger overlapping window operators.
   * @param output a partial output
   */
  @Override
  public void emit(final PartialTimeWindowOutput<V> output) {
    // save partial result into computation reuser
    spanTracker.savePartialOutput(output.windowStartTime, output.windowEndTime, output.output);
    // trigger overlapping window operators for final aggregation
    owoStage.onNext(output.windowEndTime);
  }

  @Override
  public void close() throws Exception {
  }
}