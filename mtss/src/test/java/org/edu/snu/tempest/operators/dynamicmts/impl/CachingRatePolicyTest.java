package org.edu.snu.tempest.operators.dynamicmts.impl;


import org.edu.snu.tempest.operators.Timescale;
import org.edu.snu.tempest.operators.dynamicmts.DynamicRelationCube;
import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

public class CachingRatePolicyTest {

  /**
   * CachingPolicy should not save all outputs.
   */
  @Test
  public void cachingRateZeroTest() {
    final List<Timescale> timescales = new LinkedList<>();
    timescales.add(new Timescale(4, 2));
    timescales.add(new Timescale(6, 4));
    timescales.add(new Timescale(10, 5));

    final DynamicRelationCube.CachingPolicy policy = new CachingRatePolicy(timescales, 0);
    for (int endTime = 10; endTime < 1000; endTime++) {
      for (final Timescale ts : timescales) {
        final int startTime = endTime - 10;
        // it does not save all outputs.
        Assert.assertFalse(policy.cache(startTime, endTime, ts));
      }
    }
  }

  /**
   * CachingPolicy should save outputs every window size interval.
   */
  @Test
  public void cachingRateOneTest() {
    final List<Timescale> timescales = new LinkedList<>();
    timescales.add(new Timescale(4, 2));
    timescales.add(new Timescale(6, 4));
    timescales.add(new Timescale(10, 5));

    long ts1CachingTime = 6;
    long ts2CachingTime = 4;

    final DynamicRelationCube.CachingPolicy policy = new CachingRatePolicy(timescales, 1);
    for (int endTime = 10; endTime < 1000; endTime++) {
      for (final Timescale ts : timescales) {
        final int startTime = endTime - 10;
        if (ts.windowSize == 10) {
          Assert.assertFalse(policy.cache(startTime, endTime, ts));
        } else {
          final boolean det = policy.cache(startTime, endTime, ts);
          if (det) {
            if (ts.windowSize == 4) {
              Assert.assertEquals(4, endTime - ts1CachingTime);
              ts1CachingTime = endTime;
            } else {
              Assert.assertEquals(6, endTime - ts2CachingTime);
              ts2CachingTime = endTime;
            }
          }
        }
      }
    }
  }
}
