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
package vldb.operator.window.timescale.pafas.dynamic;

import org.apache.log4j.Logger;
import vldb.operator.common.NotFoundException;
import vldb.operator.window.timescale.Timescale;
import vldb.operator.window.timescale.common.WindowTimeAndOutput;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * It holds timescale outputs for computation reuse.
 */
public final class DynamicDPOutputLookupTableImpl<V> implements DynamicOutputLookupTable<V> {
  private static final Logger LOG = Logger.getLogger(DynamicDPOutputLookupTableImpl.class.getName());

  /**
   * A data structure for saving outputs.
   */
  public final ConcurrentMap<Long, ConcurrentHashMap<Long, Map<Timescale, V>>> table;

  @Inject
  private DynamicDPOutputLookupTableImpl() {
    table = new ConcurrentHashMap<>();
  }

  /**
   * Save an output into the table.
   * @param startTime start time of the output
   * @param endTime end time of the output
   * @param output an output
   */
  @Override
  public void saveOutput(final long startTime, final long endTime, final Timescale timescale, final V output) {
    ConcurrentHashMap<Long, Map<Timescale, V>> row = table.get(startTime);
    if (row == null) {
      table.putIfAbsent(startTime, new ConcurrentHashMap<Long, Map<Timescale, V>>());
      row = table.get(startTime);
    }

    Map<Timescale, V> map = row.get(endTime);
    if (map == null) {
      map = new HashMap<>();
      row.put(endTime, map);
    }
    map.put(timescale, output);
  }

  /**
   * Lookup an output ranging from startTime to endTime.
   * @param startTime start time of the output
   * @param endTime end time of the output
   * @return an output
   * @throws vldb.operator.common.NotFoundException when it cannot find an output ranging from startTime to endTime.
   */
  @Override
  public V lookup(final long startTime, final long endTime, final Timescale ts) throws NotFoundException {
    final Map<Timescale, V> entry;
    try {
      entry = table.get(startTime).get(endTime);
    } catch (final Exception e) {
      throw new NotFoundException("Cannot find element: at (" + startTime + ", " + endTime + ")");
    }

    if (entry == null) {
      throw new NotFoundException("Cannot find element: at (" + startTime + ", " + endTime + ")");
    }

    if (entry.get(ts) == null) {
      throw new NotFoundException("Cannot find element: at (" + startTime + ", " + endTime + ")");
    }
    return entry.get(ts);
  }

  /**
   * Lookup multiple outputs which start at the startTime.
   * @param startTime start time of the outputs
   * @return outputs which start at the startTime.
   * @throws edu.snu.tempest.operator.common.NotFoundException
   */
  @Override
  public ConcurrentMap<Long, Map<Timescale, V>> lookup(final long startTime) throws NotFoundException {
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
   * @throws vldb.operator.common.NotFoundException
   */
  @Override
  public WindowTimeAndOutput<V> lookupLargestSizeOutput(final long startTime, final long endTime)
      throws NotFoundException {
    throw new RuntimeException("Not support");
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
  public void deleteOutput(final long startTime, final long endTime, final Timescale timescale){
    ConcurrentHashMap<Long, Map<Timescale, V>> row = table.get(startTime);
    if (row != null){
      row.get(endTime).remove(timescale);
      if (row.get(endTime).size() == 0) {
        row.remove(endTime);
      }
    }
  }

  @Override
  public void deleteOutput(final long startTime, final long endTime, V value){
    ConcurrentHashMap<Long, Map<Timescale, V>> row = table.get(startTime);
    if (row != null){
      final Map<Timescale, V> map = row.get(endTime);
      if (map != null) {
        for (final Map.Entry<Timescale, V> entry : map.entrySet()) {
          if (entry.getValue() == value) {
            map.remove(entry.getKey());
            break;
          }
        }
        if (map.size() == 0) {
          row.remove(endTime);
        }
      }
    }
  }
}
