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

/**
 * Partial outputs for multi-timescale window operation.
 * Sliced window operator creates partial outputs for sharing them between multiple window operation.
 * @param <V> output type
 */
final class PartialTimeWindowOutput<V> {

  /**
   * Start time of the slice.
   */
  public final long windowStartTime;

  /**
   * End time of the partial slice.
   */
  public final long windowEndTime;

  /**
   * Partial output.
   */
  public final V output;

  /**
   * Partial output for multi-timescale window operation.
   * @param windowStartTime start time of the partial result
   * @param windowEndTime end time of the partial result
   * @param output partial output
   */
  public PartialTimeWindowOutput(final long windowStartTime,
                                 final long windowEndTime,
                                 final V output) {
    this.windowStartTime = windowStartTime;
    this.windowEndTime = windowEndTime;
    this.output = output;
  }
}
