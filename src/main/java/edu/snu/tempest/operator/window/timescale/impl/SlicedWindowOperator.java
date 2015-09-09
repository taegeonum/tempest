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

import edu.snu.tempest.operator.Operator;
import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * Sliced window operator.
 * It chops input stream and aggregates the input using Aggregator.
 */
@DefaultImplementation(DefaultSlicedWindowOperator.class)
public interface SlicedWindowOperator<I, V> extends Operator<I, PartialTimeWindowOutput<V>> {
  /**
   * Slice current partial aggregation.
   * @param sliceTime slice time
   */
  void slice(long sliceTime);
}
