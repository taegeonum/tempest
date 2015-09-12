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
import edu.snu.tempest.operator.window.timescale.parameter.TimescaleString;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

public class RandomCachingPolicyTest {

  /**
   * CachingPolicy which has 0 cachingProb should not save all outputs.
   */
  @Test
  public void cachingProbZeroTest() throws InjectionException {
    final List<Timescale> timescales = new LinkedList<>();
    timescales.add(new Timescale(4, 2));
    timescales.add(new Timescale(6, 4));
    timescales.add(new Timescale(10, 5));

    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(CachingProb.class, Integer.toString(0));
    jcb.bindNamedParameter(TimescaleString.class, TimescaleParser.parseToString(timescales));
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());

    final CachingPolicy policy = injector.getInstance(RandomCachingPolicy.class);
    for (int endTime = 10; endTime < 1000; endTime++) {
      for (final Timescale ts : timescales) {
        final int startTime = endTime - 10;
        // it does not save all outputs.
        Assert.assertFalse(policy.cache(startTime, endTime, ts));
      }
    }
  }

  /**
   * CachingPolicy which has 1 cachingProb should save all of the outputs.
   */
  @Test
  public void cachingProbOneTest() throws InjectionException {
    final List<Timescale> timescales = new LinkedList<>();
    timescales.add(new Timescale(4, 2));
    timescales.add(new Timescale(6, 4));
    timescales.add(new Timescale(10, 5));

    long ts1CachingTime = 6;
    long ts2CachingTime = 4;

    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(CachingProb.class, Integer.toString(0));
    jcb.bindNamedParameter(TimescaleString.class, TimescaleParser.parseToString(timescales));
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());

    final CachingPolicy policy = injector.getInstance(RandomCachingPolicy.class);
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
