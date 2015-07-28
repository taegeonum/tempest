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
package edu.snu.tempest.operators.dynamicmts.impl;

import edu.snu.tempest.operators.Timescale;
import edu.snu.tempest.operators.common.Aggregator;
import edu.snu.tempest.operators.dynamicmts.DynamicRelationCube;
import edu.snu.tempest.operators.dynamicmts.DynamicSlicedWindowOperator;

import javax.inject.Inject;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This implementation is based on "On-the-fly Sharing for Streamed Aggregation" paper.
 * It chops input stream into paired sliced window.
 */
public final class DynamicSlicedWindowOperatorImpl<I, V> implements DynamicSlicedWindowOperator<I> {
  private static final Logger LOG = Logger.getLogger(DynamicSlicedWindowOperatorImpl.class.getName());

  /**
   * Aggregator for partial aggregation.
   */
  private final Aggregator<I, V> aggregator;

  /**
   * RelationCube for saving partial outputs.
   */
  private final DynamicRelationCube<V> relationCube;

  /**
   * SliceQueue containing next slice time.
   */
  private final PriorityQueue<SliceInfo> sliceQueue;

  /**
   * Previous slice time.
   */
  private long prevSliceTime = 0;

  /**
   * Current slice time.
   */
  private long nextSliceTime = 0;

  /**
   * A bucket for partial aggregation.
   */
  private V bucket;

  /**
   * Timescales.
   */
  private final List<Timescale> timescales;

  /**
   * Sync object for bucket.
   */
  private final Object sync = new Object();

  /**
   * DynamicSlicedWindowOperatorImpl.
   * @param aggregator an aggregator for partial aggregation
   * @param timescales an initial timescales
   * @param relationCube a relation cube for saving partial aggregation
   * @param startTime a start time of the mts operator
   */
  @Inject
  public DynamicSlicedWindowOperatorImpl(
      final Aggregator<I, V> aggregator,
      final List<Timescale> timescales,
      final DynamicRelationCube<V> relationCube,
      final Long startTime) {
    this.aggregator = aggregator;
    this.relationCube = relationCube;
    this.bucket = aggregator.init();
    this.sliceQueue = new PriorityQueue<>(10, new SliceInfoComparator());
    this.timescales = timescales;
    initializeWindowState(startTime);
    nextSliceTime = advanceWindowGetNextSlice();
  }

  /**
   * It creates a new bucket every next slice time to slice the partially aggregated data.
   * @param currTime current time
   */
  @Override
  public synchronized void onNext(final Long currTime) {
    while (nextSliceTime < currTime) {
      prevSliceTime = nextSliceTime;
      nextSliceTime = advanceWindowGetNextSlice();
    }

    LOG.log(Level.FINE, "SlicedWindow tickTime " + currTime + ", nextSlice: " + nextSliceTime);
    if (nextSliceTime == currTime) {
      LOG.log(Level.FINE, "Sliced : [" + prevSliceTime + "-" + currTime + "]");
      // create a new bucket
      synchronized (sync) {
        final V partialAggregation = bucket;
        bucket = aggregator.init();
        // saves output to RelationCube
        relationCube.savePartialOutput(prevSliceTime, nextSliceTime, partialAggregation);
      }
      prevSliceTime = nextSliceTime;
      nextSliceTime = advanceWindowGetNextSlice();
    }
  }

  /**
   * Aggregates input into the current bucket.
   * @param val input
   */
  @Override
  public void execute(final I val) {
    LOG.log(Level.FINE, "SlicedWindow aggregates input of [" +  val + "]");
    synchronized (sync) {
      bucket = aggregator.partialAggregate(bucket, val);
    }
  }

  /**
   * Update next slice time.
   * @param ts timescale
   * @param startTime current time
   */
  @Override
  public void onTimescaleAddition(final Timescale ts, final long startTime) {
    LOG.log(Level.INFO, "SlicedWindow addTimescale " + ts);
    // Add slices
    synchronized (sliceQueue) {
      final long nst = sliceQueue.peek().sliceTime;
      addSlices(startTime, ts);
      long sliceTime = sliceQueue.peek().sliceTime;
      while (sliceTime < nst) {
        sliceTime = advanceWindowGetNextSlice();
      }
    }
  }

  /**
   * Update next slice time.
   * @param ts timescale
   */
  @Override
  public void onTimescaleDeletion(final Timescale ts) {
    LOG.log(Level.INFO, "SlicedWindow removeTimescale " + ts);
    synchronized (sliceQueue) {
      for (final Iterator<SliceInfo> iterator = sliceQueue.iterator(); iterator.hasNext();) {
        final SliceInfo slice = iterator.next();
        if (slice.timescale.equals(ts)) {
          iterator.remove();
        }
      }
    }
  }

  /**
   * This method is based on "On-the-Fly Sharing " paper.
   * Similar to initializeWindowState function
   */
  private void initializeWindowState(final long startTime) {
    LOG.log(Level.INFO, "SlicedWindow initialization");
    for (final Timescale ts : timescales) {
      addSlices(startTime, ts);
    }
  }

  /**
   * Similar to addEdges function in the "On-the-Fly ... " paper.
   */
  private void addSlices(final long startTime, final Timescale ts) {
    final long pairedB = ts.windowSize % ts.intervalSize;
    final long pairedA = ts.intervalSize - pairedB;
    synchronized (sliceQueue) {
      sliceQueue.add(new SliceInfo(startTime + pairedA, ts, false));
      sliceQueue.add(new SliceInfo(startTime + pairedA + pairedB, ts, true));
    }
  }

  /**
   * Similar to advanceWindowGetNextEdge function in the "On-the-Fly ..." paper.
   */
  private long advanceWindowGetNextSlice() {
    SliceInfo info = null;
    synchronized (sliceQueue) {
      if (sliceQueue.size() == 0) {
        return 0;
      }

      final long time = sliceQueue.peek().sliceTime;
      while (time == sliceQueue.peek().sliceTime) {
        info = sliceQueue.poll();
        if (info.last) {
          addSlices(info.sliceTime, info.timescale);
        }
      }
    }
    return info.sliceTime;
  }

  private final class SliceInfo {
    public final long sliceTime;
    public final Timescale timescale;
    public final boolean last;

    SliceInfo(final long sliceTime,
              final Timescale timescale,
              final boolean last) {
      this.sliceTime = sliceTime;
      this.timescale = timescale;
      this.last = last;
    }
  }

  private final class SliceInfoComparator implements Comparator<SliceInfo> {
    @Override
    public int compare(final SliceInfo o1, final SliceInfo o2) {
      if (o1.sliceTime < o2.sliceTime) {
        return -1;
      } else if (o1.sliceTime > o2.sliceTime) {
        return 1;
      } else {
        return 0;
      }
    }
  }
}
