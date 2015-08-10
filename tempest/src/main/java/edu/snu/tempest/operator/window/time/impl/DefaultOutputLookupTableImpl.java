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
package edu.snu.tempest.operator.window.time.impl;

import edu.snu.tempest.operator.common.NotFoundException;
import org.apache.log4j.Logger;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * It holds timescale outputs for computation reuse.
 */
final class DefaultOutputLookupTableImpl<V> implements OutputLookupTable<V> {
  private static final Logger LOG = Logger.getLogger(DefaultOutputLookupTableImpl.class.getName());

  /**
   * A data structure for saving outputs.
   */
  public final ConcurrentMap<Long, ConcurrentSkipListMap<Long, V>> table;
  
  public DefaultOutputLookupTableImpl() {
    table = new ConcurrentHashMap<>();
  }

  /**
   * Save an output into the table.
   * @param startTime start time of the output
   * @param endTime end time of the output
   * @param output an output
   */
  public void saveOutput(final long startTime, final long endTime, final V output) {
    ConcurrentSkipListMap<Long, V> row = table.get(startTime);
    if (row == null) {
      table.putIfAbsent(startTime, new ConcurrentSkipListMap<Long, V>(new LongComparator()));
      row = table.get(startTime);
    }
    row.putIfAbsent(endTime, output);
  }

  /**
   * Lookup an output ranging from startTime to endTime.
   * @param startTime start time of the output
   * @param endTime end time of the output
   * @return an output
   * @throws NotFoundException when it cannot find an output ranging from startTime to endTime.
   */
  public V lookup(final long startTime, final long endTime) throws NotFoundException {
    final V entry;
    try {
      entry = table.get(startTime).get(endTime);
    } catch (final Exception e) {
      throw new NotFoundException("Cannot find element: at (" + startTime + ", " + endTime + ")");
    }
    return entry;
  }

  /**
   * Lookup multiple outputs which start at the startTime.
   * @param startTime start time of the outputs
   * @return outputs which start at the startTime.
   * @throws NotFoundException
   */
  public ConcurrentSkipListMap<Long, V> lookup(final long startTime) throws NotFoundException {
    return table.get(startTime);
  }

  /**
   * Lookup an output having largest endTime within outputs which start at startTime.
   * e.g) if this table has [s=3, e=4] [s=3, e=5], [s=3, e=7] outputs
   *      and a user calls lookupLargestSizeOutput(startTime=3, endTime=8),
   *      then it returns [s=3, e=7] which is biggest endTime at startTime=3
   * @param startTime minimum start time
   * @param endTime maximum end time
   * @return an output containing time information (startTime, endTime).
   * @throws NotFoundException
   */
  public WindowTimeAndOutput<V> lookupLargestSizeOutput(final long startTime, final long endTime)
      throws NotFoundException {
    final ConcurrentSkipListMap<Long, V> row = table.get(startTime);
    if (row == null) {
      throw new NotFoundException("Cannot lookup startTime " + startTime + " in lookupLargestSizeOutput");
    }

    final Long largestKey = row.floorKey(endTime);
    if (largestKey == null) {
      throw new NotFoundException(
          "Cannot lookup endTime " + endTime  + "from " + startTime + " in lookupLargestSizeOutput");
    } else {
      return new WindowTimeAndOutput<V>(startTime, largestKey, row.get(largestKey));
    }
  }

  /**
   * Delete outputs which start at startTime.
   * @param startTime start time of the outputs.
   */
  public void deleteOutputs(final long startTime) {
    table.remove(startTime);
  }
}
