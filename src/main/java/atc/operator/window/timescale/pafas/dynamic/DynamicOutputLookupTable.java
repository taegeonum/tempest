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
package atc.operator.window.timescale.pafas.dynamic;

import atc.operator.common.NotFoundException;
import atc.operator.window.timescale.Timescale;
import atc.operator.window.timescale.common.WindowTimeAndOutput;

import java.util.concurrent.ConcurrentMap;

/**
 * It holds timescale outputs for computation reuse.
 */
public interface DynamicOutputLookupTable<V> {

  /**
   * Save an output into the table.
   * @param startTime start time of the output
   * @param endTime end time of the output
   * @param output an output
   */
  void saveOutput(final long startTime, final long endTime, final Timescale timescale, final V output);

  /**
   * Lookup an output ranging from startTime to endTime.
   * @param startTime start time of the output
   * @param endTime end time of the output
   * @return an output
   * @throws atc.operator.common.NotFoundException when it cannot find an output ranging from startTime to endTime.
   */
  V lookup(final long startTime, final long endTime, final Timescale ts) throws NotFoundException;

  /**
   * Lookup multiple outputs which start at the startTime.
   * @param startTime start time of the outputs
   * @return outputs which start at the startTime.
   * @throws edu.snu.tempest.operator.common.NotFoundException
   */
  ConcurrentMap<Long, ConcurrentMap<Timescale, V>> lookup(final long startTime) throws NotFoundException;

  /**
   * Lookup an output having largest endTime within outputs which start at startTime.
   * e.g) if this table has [s=3, e=4] [s=3, e=5], [s=3, e=7] outputs
   *      and a user calls lookupLargestSizeOutput(startTime=3, endTime=8),
   *      then it returns [s=3, e=7] which is biggest endTime at startTime=3
   * @param startTime minimum start time
   * @param endTime maximum end time
   * @return an output containing time information (startTime, endTime).
   * @throws atc.operator.common.NotFoundException
   */
  WindowTimeAndOutput<V> lookupLargestSizeOutput(final long startTime, final long endTime)
      throws NotFoundException;

  /**
   * Delete outputs which start at startTime.
   * @param startTime start time of the outputs.
   */
  void deleteOutputs(final long startTime);

  /**
   * Delete output that starts at startTime and ends at endTime.
   * @param startTime start time of the output
   * @param endTime end time of the output
   */
  void deleteOutput(final long startTime, final long endTime, final Timescale timescale);

  void deleteOutput(final long startTime, final long endTime, V value);
}
