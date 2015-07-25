package org.edu.snu.tempest.operators.dynamicmts.impl;

import org.edu.snu.tempest.operators.Timescale;
import org.edu.snu.tempest.operators.common.OutputLookupTable;
import org.edu.snu.tempest.operators.dynamicmts.DynamicRelationCube;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * It deletes items on OutputLookupTable
 * if they are unnecessary.
 *
 * When the largest window size cannot reach outputs,
 * the outputs are removed by GC.
 */
public final class DefaultGarbageCollectorImpl implements DynamicRelationCube.GarbageCollector {
  private static final Logger LOG = Logger.getLogger(DefaultGarbageCollectorImpl.class.getName());

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
  
  public DefaultGarbageCollectorImpl(final Collection<Timescale> timescales,
                                     final OutputLookupTable<?> table,
                                     final long startTime) {
    this.table = table;
    this.timescales = new LinkedList<>(timescales);
    largestWindowSize = new AtomicLong(findLargestWindowSize());
    preVReachableTime = startTime;
  }
  
  public DefaultGarbageCollectorImpl(final OutputLookupTable<Map<?, ?>> table,
                                     final long startTime) {
    this(new ConcurrentLinkedQueue<Timescale>(), table, startTime);
  }

  /**
   * If outputs cannot be reached based on the current time,
   * GC removes the outputs.
   * In this implementation, it removes outputs which start from (current_time - largest_window_size)
   * because the outputs cannot be reachable at current time.
   * @param time current time
   */
  @Override
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
   * @param currTime current time
   */
  @Override
  public void onTimescaleAddition(final Timescale ts, final long currTime) {
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
   * @param ts timescale
   */
  @Override
  public void onTimescaleDeletion(final Timescale ts) {
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
