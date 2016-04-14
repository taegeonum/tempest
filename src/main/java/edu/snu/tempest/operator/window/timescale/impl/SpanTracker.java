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

import edu.snu.tempest.operator.window.timescale.Timescale;

/**
 * Computation reuser interface. This is a data structure for output generation of window output.
 * It saves partial aggregation and produces final aggregation of window output.
 */
public interface SpanTracker<T> extends AutoCloseable {

  /**
   * Save a partial output containing data starting from the startTime to endTime.
   * @param startTime start time of the output
   * @param endTime end time of the output
   * @param output output
   */
  void savePartialOutput(long startTime, long endTime, T output);

  /**
   * Produce an output containing data starting from the startTime to endTime.
   * @param startTime start time of the output
   * @param endTime end time of the output
   * @param ts timescale
   * @return an aggregated output ranging from startTime to endTime.
   */
  DepOutputAndResult<T> finalAggregate(long startTime, long endTime, Timescale ts);

  /**
   * Save output information to track dependencies between outputs.
   * This function should be called before finalAggregate.
   * @param startTime start time of the output
   * @param endTime end time of the output
   * @param ts timescale
   */
  void saveOutputInformation(long startTime, long endTime, Timescale ts);

  /**
   * Receive timescale to be added.
   * @param ts timescale to be added.
   * @param addTime the time when timescale is added.
   */
  void onTimescaleAddition(Timescale ts, long addTime);

  /**
   * Receive timescale to be deleted.
   * @param ts timescale to be deleted.
   * @param deleteTime the time when timescale is deleted.
   */
  void onTimescaleDeletion(Timescale ts, long deleteTime);
}
