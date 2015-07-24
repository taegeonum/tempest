package org.edu.snu.tempest.operators.dynamicmts.impl;

import org.apache.reef.tang.annotations.Parameter;
import org.edu.snu.tempest.operators.Timescale;
import org.edu.snu.tempest.operators.dynamicmts.DynamicRelationCube;
import org.edu.snu.tempest.operators.parameters.CachingRate;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * CachingRatePolicy caches outputs according to the caching rate.
 * Also, it does not cache outputs of a timescale which has largest window size
 * because outputs of larges window size cannot be used for outputs of lower window size.
 */
public final class CachingRatePolicy implements DynamicRelationCube.CachingPolicy {

  private final List<Timescale> timescales;

  /**
   * Largest window size.
   * DynamicRelationCubeImpl doesn't save outputs generated from a timescale
   * which has largest window size.
   */
  private AtomicLong largestWindowSize;

  /**
   * Contains a map of timescale and latest caching time of an output of the timescale.
   * DynamicRelationCubeImpl decides to cache or not to cache an output
   * with this information.
   */
  private final ConcurrentHashMap<Timescale, Long> latestCachingTimeMap;

  /**
   * CachingRate decides the amount of finalAggregation to save.
   */
  private final double cachingRate;

  @Inject
  public CachingRatePolicy(List<Timescale> timescales,
                           @Parameter(CachingRate.class) final double cachingRate) {
    this.timescales = new LinkedList<>(timescales);
    this.cachingRate = cachingRate;
    this.latestCachingTimeMap = new ConcurrentHashMap<>();
    this.largestWindowSize = new AtomicLong(findLargestWindowSize());
  }

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
    // if the cachingRate is zero, then cachingPeriod is infinite.
    // if the cachingRate is one, then cachingPeriod is windowSize.
    final double cachingPeriod = ts.windowSize / cachingRate;
    if ((endTime - latestCachingTime) >= cachingPeriod
        && ts.windowSize < this.largestWindowSize.get()) {
      latestCachingTimeMap.put(ts, endTime);
      return true;
    } else {
      return false;
    }
  }

  @Override
  public void onTimescaleAddition(final Timescale ts, final long startTime) {
    synchronized (this.timescales) {
      this.timescales.add(ts);
      if (ts.windowSize > this.largestWindowSize.get()) {
        this.largestWindowSize.set(ts.windowSize);
      }
    }
  }

  @Override
  public void onTimescaleDeletion(final Timescale ts) {
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
