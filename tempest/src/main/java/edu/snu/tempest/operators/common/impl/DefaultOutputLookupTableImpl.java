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
package edu.snu.tempest.operators.common.impl;

import edu.snu.tempest.operators.common.NotFoundException;
import edu.snu.tempest.operators.common.OutputLookupTable;
import org.apache.log4j.Logger;

import javax.inject.Inject;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * It saves outputs into Table.
 */
public final class DefaultOutputLookupTableImpl<V> implements OutputLookupTable<V> {
  private static final Logger LOG = Logger.getLogger(DefaultOutputLookupTableImpl.class.getName());

  /**
   * A data structure for saving outputs.
   */
  public final ConcurrentMap<Long, ConcurrentSkipListMap<Long, V>> table;
  
  @Inject
  public DefaultOutputLookupTableImpl() {
    table = new ConcurrentHashMap<>();
  }

  /**
   * Save an output into the table.
   * @param startTime start time of the output
   * @param endTime end time of the output
   * @param output an output
   */
  @Override
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
   * @throws NotFoundException
   */
  @Override
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
  @Override
  public ConcurrentSkipListMap<Long, V> lookup(final long startTime) throws NotFoundException {
    return table.get(startTime);
  }

  /**
   * Lookup an output having largest endTime within outputs which start at startTime.
   * @param startTime minimum start time
   * @param endTime maximum end time
   * @return an output containing time information (startTime, endTime).
   * @throws NotFoundException
   */
  @Override
  public TimeAndOutput<V> lookupLargestSizeOutput(final long startTime, final long endTime) throws NotFoundException {
    final ConcurrentSkipListMap<Long, V> row = table.get(startTime);
    if (row == null) {
      throw new NotFoundException("Cannot lookup startTime " + startTime + " in lookupLargestSizeOutput");
    }

    final Long largestKey = row.floorKey(endTime);
    if (largestKey == null) {
      throw new NotFoundException(
          "Cannot lookup endTime " + endTime  + "from " + startTime + " in lookupLargestSizeOutput");
    } else {
      return new TimeAndOutput<V>(startTime, largestKey, row.get(largestKey));
    }
  }

  /**
   * Delete outputs which start at startTime.
   * @param startTime start time of the outputs.
   */
  @Override
  public void deleteOutputs(final long startTime) {
    table.remove(startTime);
  }
}
