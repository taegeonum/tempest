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
package edu.snu.tempest.operator.window.timescale.impl;

import edu.snu.tempest.operator.window.timescale.Timescale;
import edu.snu.tempest.operator.window.timescale.parameter.StartTime;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/** This implementation is based on "On-the-fly Sharing for Streamed Aggregation" paper.
 * Reference: S. Krishnamurthy, C. Wu, and M. Franklin. On-the-fly sharing
 * for streamed aggregation. In ACM SIGMOD, 2006
 * It returns next slice time for slicing input stream into paired sliced window.
 */
public final class StaticNextSliceTimeProvider implements NextSliceTimeProvider {
  private static final Logger LOG = Logger.getLogger(StaticNextSliceTimeProvider.class.getName());

  /**
   * The list of timescale.
   */
  private final List<Timescale> timescales;

  /**
   * A queue containing next slice time.
   */
  private final List<Long> sliceQueue;

  /**
   * A period of the repeated pattern.
   */
  private final long period;

  /**
   * An index for looking up sliceQueue.
   */
  private int index = 0;

  /**
   * A start time.
   */
  private final long startTime;

  /**
   * A previous slice time.
   */
  private long prevSliceTime = 0;

  /**
   * StaticComputationReuserImpl Implementation.
   * @param tsParser timescale parser
   * @param startTime an initial start time
   */
  @Inject
  private StaticNextSliceTimeProvider(final TimescaleParser tsParser,
                                      @Parameter(StartTime.class) final long startTime) {
    this.timescales = tsParser.timescales;
    this.sliceQueue = new LinkedList<>();
    this.period = calculatePeriod(timescales);
    this.startTime = startTime;
    LOG.log(Level.INFO, StaticNextSliceTimeProvider.class + " started. PERIOD: " + period);
    createSliceQueue();
  }

  /**
   * Gets next slice time for slicing input and creating partial outputs.
   * This can be used for slice time to create partial results of incremental aggregation.
   * SlicedWindowOperator can use this to slice input.
   */
  @Override
  public synchronized long nextSliceTime() {
    long sliceTime = adjustNextSliceTime();
    while (prevSliceTime == sliceTime) {
      sliceTime = adjustNextSliceTime();
    }
    prevSliceTime = sliceTime;
    return sliceTime;
  }

  /**
   * Adjust next slice time.
   */
  private long adjustNextSliceTime() {
    return startTime + (index / sliceQueue.size()) * period
        + sliceQueue.get((index++) % sliceQueue.size());
  }

  @Override
  public void onTimescaleAddition(final Timescale ts, final long addTime) {
    throw new RuntimeException("Not supported");
  }
  
  @Override
  public void onTimescaleDeletion(final Timescale ts, final long deleteTime) {
    throw new RuntimeException("Not supported");
  }

  /**
   * It creates the list of next slice time.
   * This method is based on "On-the-Fly Sharing for Streamed Aggregation" paper.
   * Similar to initializeWindowState function
   */
  private void createSliceQueue() {
    // add sliced window edges
    for (final Timescale ts : timescales) {
      final long pairedB = ts.windowSize % ts.intervalSize;
      final long pairedA = ts.intervalSize - pairedB;
      long time = pairedA;
      boolean odd = true;

      while(time <= period) {
        sliceQueue.add(time);
        if (odd) {
          time += pairedB;
        } else {
          time += pairedA;
        }
        odd = !odd;
      }
    }
    Collections.sort(sliceQueue);
    LOG.log(Level.FINE, "Sliced queue: " + sliceQueue);
  }

  /**
   * Find period of repeated pattern.
   * period = c * lcm ( i_{1}, i_{2}, ..., i_{k} ) ( i_{k} is interval of k-th timescale)
   * c is natural number which satisfies period >= largest_window_size
   */
  public static long calculatePeriod(final List<Timescale> timescales) {
    long period = 0;
    long largestWindowSize = 0;

    for (final Timescale ts : timescales) {
      if (period == 0) {
        period = ts.intervalSize;
      } else {
        period = lcm(period, ts.intervalSize);
      }
      // find largest window size
      if (largestWindowSize < ts.windowSize) {
        largestWindowSize = ts.windowSize;
      }
    }

    if (period < largestWindowSize) {
      final long div = largestWindowSize / period;
      if (largestWindowSize % period == 0) {
        period *= div;
      } else {
        period *= (div+1);
      }
    }
    return period;
  }

  private static long gcd(long a, long b) {
    while (b > 0) {
      final long temp = b;
      b = a % b; // % is remainder
      a = temp;
    }
    return a;
  }

  private static long lcm(final long a, final long b) {
    return a * (b / gcd(a, b));
  }
}