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
package atc.operator.window.timescale.pafas;

import org.apache.log4j.Logger;
import atc.operator.common.NotFoundException;
import atc.operator.window.timescale.common.OutputLookupTable;
import atc.operator.window.timescale.common.WindowTimeAndOutput;

import javax.inject.Inject;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * It holds timescale outputs for computation reuse.
 */
public final class DPOutputLookupTableImpl<V> implements OutputLookupTable<V> {
  private static final Logger LOG = Logger.getLogger(DPOutputLookupTableImpl.class.getName());

  /**
   * A data structure for saving outputs.
   */
  public final ConcurrentMap<Long, ConcurrentHashMap<Long, V>> table;

  @Inject
  private DPOutputLookupTableImpl() {
    table = new ConcurrentHashMap<>();
  }

  /**
   * Save an output into the table.
   * @param startTime start time of the output
   * @param endTime end time of the output
   * @param output an output
   */
  public void saveOutput(final long startTime, final long endTime, final V output) {
    ConcurrentHashMap<Long, V> row = table.get(startTime);
    if (row == null) {
      table.putIfAbsent(startTime, new ConcurrentHashMap<Long, V>());
      row = table.get(startTime);
    }
    row.putIfAbsent(endTime, output);
  }

  /**
   * Lookup an output ranging from startTime to endTime.
   * @param startTime start time of the output
   * @param endTime end time of the output
   * @return an output
   * @throws atc.operator.common.NotFoundException when it cannot find an output ranging from startTime to endTime.
   */
  public V lookup(final long startTime, final long endTime) throws NotFoundException {
    final V entry;
    try {
      entry = table.get(startTime).get(endTime);
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
   * @param startTime start time of the outputs
   * @return outputs which start at the startTime.
   * @throws edu.snu.tempest.operator.common.NotFoundException
   */
  public ConcurrentMap<Long, V> lookup(final long startTime) throws NotFoundException {
    if (table.get(startTime) == null) {
      throw new NotFoundException("Not found startTime: " + startTime);
    }
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
   * @throws atc.operator.common.NotFoundException
   */
  public WindowTimeAndOutput<V> lookupLargestSizeOutput(final long startTime, final long endTime)
      throws NotFoundException {
    throw new RuntimeException("Not supported");
  }

  /**
   * Delete outputs which start at startTime.
   * @param startTime start time of the outputs.
   */
  @Override
  public void deleteOutputs(final long startTime) {
    table.remove(startTime);
  }

  /**
   * Delete output that starts at startTime and ends at endTime.
   * @param startTime start time of the output
   * @param endTime end time of the output
   */
  @Override
  public void deleteOutput(final long startTime, final long endTime){
    ConcurrentMap<Long, V> row = table.get(startTime);
    if (row != null){
      row.remove(endTime);
    }
  }
}
