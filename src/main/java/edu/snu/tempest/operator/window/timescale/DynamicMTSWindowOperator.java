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
package edu.snu.tempest.operator.window.timescale;

import edu.snu.tempest.operator.window.timescale.impl.DynamicMTSOperatorImpl;
import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * Dynamic multi-time scale WindowOperator interface.
 * It receives input and produces window output every interval.
 */
@DefaultImplementation(DynamicMTSOperatorImpl.class)
public interface DynamicMTSWindowOperator<I, V> extends TimescaleWindowOperator<I, V> {

  /**
   * Receive timescale to be added.
   * @param ts timescale to be added.
   * @param addTime the time when timescale is added. Time unit is in seconds.
   */
  void onTimescaleAddition(Timescale ts, long addTime);

  /**
   * Receive timescale to be deleted.
   * @param ts timescale to be deleted.
   * @param deleteTime the time when timescale is deleted. Time unit is in seconds.
   */
  void onTimescaleDeletion(Timescale ts, long deleteTime);
}
