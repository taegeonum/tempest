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
import edu.snu.tempest.operator.window.timescale.parameter.CachingProb;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.security.InvalidParameterException;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * RandomCachingPolicy caches outputs according to the caching probability.
 * Also, it does not cache outputs of a timescale which has largest window size
 * because outputs of largest window size cannot be used for outputs of smaller window size.
 */
public final class RandomCachingPolicy implements CachingPolicy {

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
   * CachingProb decides the amount of finalAggregation to be saved.
   */
  private final double cachingProb;

  /**
   * Random value to decide output caching.
   */
  private final Random random;

  /**
   * RandomCachingPolicy caches outputs according to the caching probability.
   * @param tsParser timescale parser.
   * @param cachingProb a caching probability.
   */
  @Inject
  private RandomCachingPolicy(final TimescaleParser tsParser,
                              @Parameter(CachingProb.class) final double cachingProb) {
    if (cachingProb < 0 || cachingProb > 1) {
      throw new InvalidParameterException(
          "CachingProb should be greater or equal than 0 and smaller or equal than 1: " + cachingProb);
    }
    this.timescales = new LinkedList<>(tsParser.timescales);
    this.cachingProb = cachingProb;
    this.largestWindowSize = new AtomicLong(findLargestWindowSize());
    this.random = new Random();
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
    if (ts.windowSize == this.largestWindowSize.get()) {
      // do not save largest window output
      return false;
    } else {
      return random.nextDouble() < cachingProb;
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
