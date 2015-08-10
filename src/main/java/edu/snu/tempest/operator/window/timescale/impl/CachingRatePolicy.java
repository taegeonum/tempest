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
import edu.snu.tempest.operator.window.timescale.TimescaleParser;
import edu.snu.tempest.operator.window.timescale.parameter.CachingRate;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * CachingRatePolicy caches outputs according to the caching rate.
 * Also, it does not cache outputs of a timescale which has largest window size
 * because outputs of largest window size cannot be used for outputs of smaller window size.
 */
public final class CachingRatePolicy implements CachingPolicy {

  /**
   * An initial timescales.
   */
  private final List<Timescale> timescales;

  /**
   * Largest window size.
   * This doesn't save outputs generated from a timescale
   * which has largest window size.
   */
  private AtomicLong largestWindowSize;

  /**
   * Contains a map of timescale and latest caching time of an output of the timescale.
   * This decides to cache or not to cache an output
   * with this information.
   */
  private final ConcurrentHashMap<Timescale, Long> latestCachingTimeMap;

  /**
   * CachingRate decides the amount of finalAggregation to save.
   */
  private final double cachingRate;

  /**
   * CachingRatePolicy caches outputs according to the caching rate.
   * @param tsParser timescale parser.
   * @param cachingRate a caching rate. cachingPeriod is a function of cachingRate.
   */
  @Inject
  private CachingRatePolicy(final TimescaleParser tsParser,
                            @Parameter(CachingRate.class) final double cachingRate) {
    this.timescales = new LinkedList<>(tsParser.timescales);
    this.cachingRate = cachingRate;
    this.latestCachingTimeMap = new ConcurrentHashMap<>();
    this.largestWindowSize = new AtomicLong(findLargestWindowSize());
  }

  /**
   * Decide to cache or not the output of ts ranging from startTime to endTime.
   * @param startTime start time of the output
   * @param endTime end time of the output
   * @param ts the timescale of the output
   * @return cache or not
   */
  @Override
  public boolean cache(final long startTime, final long endTime, final Timescale ts) {
    Long latestCachingTime = latestCachingTimeMap.get(ts);
    if (latestCachingTime == null) {
      latestCachingTime = 0L;
    }

    if (cachingRate == 0) {
      return false;
    }

    // cachingPeriod is a function of cachingRate.
    // cachingPeriod = windowSize / cachingRate
    // if the cachingRate is zero, then cachingPeriod is infinite and no data is cached.
    // if the cachingRate is one, then cachingPeriod is windowSize and the result is cached every windowSize.
    final double cachingPeriod = ts.windowSize / cachingRate;
    if ((endTime - latestCachingTime) >= cachingPeriod
        && ts.windowSize < this.largestWindowSize.get()) {
      latestCachingTimeMap.put(ts, endTime);
      return true;
    } else {
      return false;
    }
  }

  /**
   * Update largest window size.
   * @param ts timescale to be added.
   * @param addTime the time when timescale is added.
   */
  @Override
  public void onTimescaleAddition(final Timescale ts, final long addTime) {
    synchronized (this.timescales) {
      this.timescales.add(ts);
      if (ts.windowSize > this.largestWindowSize.get()) {
        this.largestWindowSize.set(ts.windowSize);
      }
    }
  }

  /**
   * Update largest window size.
   * @param ts timescale to be deleted.
   * @param deleteTime the time when timescale is deleted.
   */
  @Override
  public void onTimescaleDeletion(final Timescale ts, final long deleteTime) {
    synchronized (this.timescales) {
      this.timescales.remove(ts);
      if (ts.windowSize == this.largestWindowSize.get()) {
        this.largestWindowSize.set(findLargestWindowSize());
      }
    }
  }

  private long findLargestWindowSize() {
    long result = 0;
    for (final Timescale ts : timescales) {
      if (result < ts.windowSize) {
        result = ts.windowSize;
      }
    }
    return result;
  }
}
