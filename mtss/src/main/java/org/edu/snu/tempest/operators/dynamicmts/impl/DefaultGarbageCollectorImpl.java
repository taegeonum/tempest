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

  private final AtomicLong largestWindowSize;
  private final OutputLookupTable<?> table;
  private final Collection<Timescale> timescales;
  private long prevDeletedRow;
  
  public DefaultGarbageCollectorImpl(final Collection<Timescale> timescales,
                                     final OutputLookupTable<?> table,
                                     final long startTime) {
    this.table = table;
    this.timescales = new LinkedList<>(timescales);
    largestWindowSize = new AtomicLong(findLargestWindowSize());
    prevDeletedRow = startTime;
  }
  
  public DefaultGarbageCollectorImpl(final OutputLookupTable<Map<?, ?>> table,
                                     final long startTime) {
    this(new ConcurrentLinkedQueue<Timescale>(), table, startTime);
  }
  
  @Override
  public void onNext(final Long time) {
    final long deleteRow = time - largestWindowSize.get() - 1;
    if (deleteRow >= 0) {
      for (; prevDeletedRow <= deleteRow; prevDeletedRow++) {
        LOG.log(Level.FINE, "GC remove " + prevDeletedRow + " startTime row at currentTime " + time);
        table.deleteRow(prevDeletedRow);
      }
    }
  }

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
