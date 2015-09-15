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
import edu.snu.tempest.operator.window.timescale.Timescale;
import edu.snu.tempest.operator.window.timescale.TimescaleWindowOutput;


/**
 * OverlappingWindowOperator.
 * It triggers final aggregation every interval
 * For example, if a timescale is [w=10s, i=3s],
 * then OWO produces an output with 10 seconds window size every 3 seconds.
 */
public interface OverlappingWindowOperator<V> extends Operator<Long, TimescaleWindowOutput<V>> {
  /**
   * Return a timescale related to this overlapping window operator.
   *
   * @return timescale.
   */
  Timescale getTimescale();

  /**
   * Saves output information before executing final aggregation.
   * This function should be called before .onNext(time)
   * @param currTime current time
   */
  void saveOutputInformation(final Long currTime);
}