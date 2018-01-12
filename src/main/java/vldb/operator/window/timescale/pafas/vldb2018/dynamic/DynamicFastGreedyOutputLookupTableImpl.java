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
package vldb.operator.window.timescale.pafas.vldb2018.dynamic;

import org.apache.log4j.Logger;
import vldb.operator.common.NotFoundException;
import vldb.operator.window.timescale.common.LongComparator;
import vldb.operator.window.timescale.common.OutputLookupTable;
import vldb.operator.window.timescale.common.WindowTimeAndOutput;

import javax.inject.Inject;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * It holds timescale outputs for computation reuse.
 */
public final class DynamicFastGreedyOutputLookupTableImpl<V> implements OutputLookupTable<V> {
  private static final Logger LOG = Logger.getLogger(DynamicFastGreedyOutputLookupTableImpl.class.getName());

  /**
   * A data structure for saving outputs.
   */
  public final ConcurrentMap<Long, ConcurrentSkipListMap<Long, V>> table;

  @Inject
  private DynamicFastGreedyOutputLookupTableImpl() {
    table = new ConcurrentHashMap<>();
  }

  /**
   * Save an output into the table.
   * @param startTime start time of the output
   * @param endTime end time of the output
   * @param output an output
   */
  public void saveOutput(final long startTime, final long endTime, final V output) {
    ConcurrentSkipListMap<Long, V> row = table.get(endTime);
    if (row == null) {
      table.putIfAbsent(endTime, new ConcurrentSkipListMap<Long, V>(new LongComparator()));
      row = table.get(endTime);
    }
    row.putIfAbsent(startTime, output);
  }

  /**
   * Lookup an output ranging from startTime to endTime.
   * @param startTime start time of the output
   * @param endTime end time of the output
   * @return an output
   * @throws vldb.operator.common.NotFoundException when it cannot find an output ranging from startTime to endTime.
   */
  public V lookup(final long startTime, final long endTime) throws NotFoundException {
    final V entry;
    try {
      entry = table.get(endTime).get(startTime);
    } catch (final Exception e) {
      throw new NotFoundException("Cannot find element: at (" + startTime + ", " + endTime + ")");
    }

    if (entry == null) {
      throw new NotFoundException("Cannot find element: at (" + startTime + ", " + endTime + ")");
    }
    return entry;
  }

  /**
   * Lookup multiple outputs which start at the startTime.
   * @param endTime end time of the outputs
   * @return outputs which start at the endTime.
   * @throws edu.snu.tempest.operator.common.NotFoundException
   */
  public ConcurrentMap<Long, V> lookup(final long endTime) throws NotFoundException {
    if (table.get(endTime) == null) {
      throw new NotFoundException("Not found endTime: " + endTime);
    }
    return table.get(endTime);
  }

  /**
   * Lookup an output having largest endTime within outputs which start at startTime.
   * e.g) if this table has [s=3, e=4] [s=3, e=5], [s=3, e=7] outputs
   *      and a user calls lookupLargestSizeOutput(startTime=3, endTime=8),
   *      then it returns [s=3, e=7] which is biggest endTime at startTime=3
   * @param startTime minimum start time
   * @param endTime maximum end time
   * @return an output containing time information (startTime, endTime).
   * @throws vldb.operator.common.NotFoundException
   */
  public WindowTimeAndOutput<V> lookupLargestSizeOutput(final long startTime, final long endTime)
      throws NotFoundException {
    final ConcurrentSkipListMap<Long, V> row = table.get(endTime);
    if (row == null) {
      throw new NotFoundException("Cannot lookup endTime " + endTime + " in lookupLargestSizeOutput");
    }

    final Long lowestKey = row.ceilingKey(startTime);
    if (lowestKey == null) {
      throw new NotFoundException("not found " + startTime + "-" + endTime);
    }

    return new WindowTimeAndOutput<>(lowestKey, endTime, row.get(lowestKey));
  }

  /**
   * Delete outputs which start at startTime.
   * @param startTime start time of the outputs.
   */
  @Override
  public void deleteOutputs(final long endTime) {
    table.remove(endTime);
  }

  /**
   * Delete output that starts at startTime and ends at endTime.
   * @param startTime start time of the output
   * @param endTime end time of the output
   */
  @Override
  public void deleteOutput(final long startTime, final long endTime){
    ConcurrentMap<Long, V> row = table.get(endTime);
    if (row != null){
      row.remove(startTime);
    }
  }
}
