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
package vldb.operator.window.timescale.common;

import org.apache.reef.tang.annotations.DefaultImplementation;
import vldb.operator.common.NotFoundException;

import java.util.concurrent.ConcurrentMap;

/**
 * OutputLookupTable interface.
 * Data structure for saving timescale outputs.
 */
@DefaultImplementation(DefaultOutputLookupTableImpl.class)
public interface OutputLookupTable<V> {

  /**
   * Save an output ranging from startTime to endTime.
   *
   * @param startTime start time of the output
   * @param endTime   end time of the output
   * @param output    an output
   */
  void saveOutput(long startTime, long endTime, final V output);

  /**
   * Lookup an output ranging from startTime to endTime.
   *
   * @param startTime start time of the output
   * @param endTime   end time of the output
   * @return an output
   * @throws NotFoundException throws NotFoundException
   *                           when it cannot find an output ranging from startTime to endTime.
   */
  V lookup(long startTime, long endTime) throws NotFoundException;

  /**
   * Lookup multiple outputs which start at the startTime.
   *
   * @param startTime start time of the outputs
   * @return outputs which start at the startTime.
   * @throws NotFoundException throws NotFoundException
   *                           when it cannot find an output starting at startTime.
   */
  ConcurrentMap<Long, V> lookup(long startTime) throws NotFoundException;

  /**
   * Lookup an output having largest endTime within outputs which start at startTime.
   * e.g) if this table has [s=3, e=4] [s=3, e=5], [s=3, e=7] outputs
   * and a user calls lookupLargestSizeOutput(startTime=3, endTime=8),
   * then it returns [s=3, e=7] which is biggest endTime at startTime=3
   *
   * @param startTime minimum start time
   * @param endTime   maximum end time
   * @return TimeAndValue this contains value and time information.
   * @throws NotFoundException throws NotFoundException
   *                           when it cannot find an output ranging from startTime to endTime.
   */
  WindowTimeAndOutput<V> lookupLargestSizeOutput(long startTime, long endTime) throws NotFoundException;

  /**
   * Delete outputs which start at startTime.
   *
   * @param startTime start time of the outputs.
   */
  void deleteOutputs(long startTime);

  /**
   * Delete an output of [start-end].
   *
   * @param startTime start time
   * @param endTime   end time
   */
  void deleteOutput(long startTime, long endTime);
}