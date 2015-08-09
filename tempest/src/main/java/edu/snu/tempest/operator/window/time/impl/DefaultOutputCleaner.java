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

import edu.snu.tempest.operator.window.time.Timescale;

import java.util.Collection;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * It deletes items on OutputLookupTable
 * if they are unnecessary.
 *
 * When the largest window size cannot reach outputs,
 * the outputs are removed by output cleaner.
 */
final class DefaultOutputCleaner {
  private static final Logger LOG = Logger.getLogger(DefaultOutputCleaner.class.getName());

  /**
   * Largest window size in timescales.
   */
  private final AtomicLong largestWindowSize;

  /**
   * Table for deleting rows.
   */
  private final OutputLookupTable<?> table;

  /**
   * Timescales.
   */
  private final Collection<Timescale> timescales;

  /**
   * Previous reachable time.
   */
  private long preVReachableTime;

  /**
   * DynamicOutputCleaner.
   * @param timescales an initial timescales
   * @param table an output lookup table
   * @param startTime an initial start time
   */
  public DefaultOutputCleaner(final Collection<Timescale> timescales,
                              final OutputLookupTable<?> table,
                              final long startTime) {
    this.table = table;
    this.timescales = new LinkedList<>(timescales);
    largestWindowSize = new AtomicLong(findLargestWindowSize());
    preVReachableTime = startTime;
  }
  
  public DefaultOutputCleaner(final DefaultOutputLookupTableImpl<?> table,
                              final long startTime) {
    this(new ConcurrentLinkedQueue<Timescale>(), table, startTime);
  }

  /**
   * If outputs cannot be reached based on the current time,
   * OutputCleaner removes the outputs.
   * In this implementation, it removes outputs which start from (current_time - largest_window_size)
   * because the outputs cannot be reachable at current time.
   * @param time current time
   */
  public void onNext(final Long time) {
    final long reachableTime = time - largestWindowSize.get();
    if (reachableTime >= 0) {
      for (; preVReachableTime < reachableTime; preVReachableTime++) {
        LOG.log(Level.FINE, "GC remove outputs starting from" + preVReachableTime + " at " + time);
        table.deleteOutputs(preVReachableTime);
      }
    }
  }

  /**
   * Update the largest window size.
   * @param ts timescale
   * @param addTime the time when timescale is added.
   */
  public void onTimescaleAddition(final Timescale ts, final long addTime) {
    LOG.log(Level.FINE, "GC add timescale " + ts);
    if (largestWindowSize.get() < ts.windowSize) {
      largestWindowSize.set(ts.windowSize);
    }
    synchronized (timescales) {
      timescales.add(ts);
    }
  }

  /**
   * Update the largest window size.
   * @param ts timescale to be deleted.
   * @param deleteTime the time when timescale is deleted.
   * */
  public void onTimescaleDeletion(final Timescale ts, final long deleteTime) {
    LOG.log(Level.FINE, "GC remove timescale " + ts);
    synchronized (timescales) {
      timescales.remove(ts);
    }
    largestWindowSize.set(findLargestWindowSize());
  }

  private long findLargestWindowSize() {
    long window = 0;
    for (final Timescale ts : timescales) {
      if (window < ts.windowSize) {
        window = ts.windowSize;
      }
    }
    return window;
  }
}
