/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package edu.snu.tempest.operator.window.time.impl;

import edu.snu.tempest.operator.window.time.Timescale;

/**
 * Computation reuser interface. This is a data structure for output generation of window output.
 * It saves partial aggregation and produces final aggregation of window output.
 */
public interface ComputationReuser<T> extends TimescaleSignalListener {

  /**
   * Get next slice time for partial aggregation.
   * @return next slice time
   */
  long nextSliceTime();

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
  T finalAggregate(long startTime, long endTime, Timescale ts);
}
