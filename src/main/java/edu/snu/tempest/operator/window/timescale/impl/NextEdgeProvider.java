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
 * Provide next slice time for partial aggregation.
 */
public interface NextEdgeProvider {

  /**
   * Get next slice time for partial aggregation.
   * This can be used for slice time to create partial results of incremental aggregation.
   * @return next slice time
   */
  long nextSliceTime();

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