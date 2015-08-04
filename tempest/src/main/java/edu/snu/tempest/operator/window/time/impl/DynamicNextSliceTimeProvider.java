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
package edu.snu.tempest.operator.window.time.impl;

import edu.snu.tempest.operator.window.time.Timescale;
import edu.snu.tempest.operator.window.time.TimescaleParser;
import edu.snu.tempest.operator.window.time.parameter.StartTime;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This implementation is based on "On-the-fly Sharing for Streamed Aggregation" paper.
 * Reference: S. Krishnamurthy, C. Wu, and M. Franklin. On-the-fly sharing
 * for streamed aggregation. In ACM SIGMOD, 2006
 * It returns next slice time for slicing input stream into paired sliced window.
 */
public final class DynamicNextSliceTimeProvider implements NextSliceTimeProvider {
  private static final Logger LOG = Logger.getLogger(DynamicNextSliceTimeProvider.class.getName());

  /**
   * SliceQueue containing next slice time.
   */
  private final PriorityQueue<SliceInfo> sliceQueue;

  /**
   * Timescales.
   */
  private final List<Timescale> timescales;

  /**
   * DynamicNextSliceTimeProvider.
   * @param tsParser timescale parser
   * @param startTime a start time of the mts operator
   */
  @Inject
  private DynamicNextSliceTimeProvider(
      final TimescaleParser tsParser,
      @Parameter(StartTime.class) final Long startTime) {
    this.sliceQueue = new PriorityQueue<>(10, new SliceInfoComparator());
    this.timescales = tsParser.timescales;
    initializeWindowState(startTime);
  }

  /**
   * It returns a next slice time for producing partial results.
   * Similar to advanceWindowGetNextEdge function in the "On-the-Fly ..." paper.
   */
  @Override
  public long nextSliceTime() {
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

  /**
   * Update next slice time.
   * @param ts timescale
   * @param addTime the time when timescale is added.
   */
  @Override
  public void onTimescaleAddition(final Timescale ts, final long addTime) {
    LOG.log(Level.INFO, "SlicedWindow addTimescale " + ts);
    // Add slices
    synchronized (sliceQueue) {
      final long nst = sliceQueue.peek().sliceTime;
      addSlices(addTime, ts);
      long sliceTime = sliceQueue.peek().sliceTime;
      while (sliceTime < nst) {
        sliceTime = nextSliceTime();
      }
    }
  }

  /**
   * Update next slice time.
   * @param ts timescale
   * @param deleteTime the time when timescale is deleted.
   */
  @Override
  public void onTimescaleDeletion(final Timescale ts, final long deleteTime) {
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
   * Initialize next slice time.
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
   * Add next slice time into sliceQueue.
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
