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

import edu.snu.tempest.operator.window.aggregator.Aggregator;
import edu.snu.tempest.operator.window.aggregator.CAAggregator;
import edu.snu.tempest.operator.window.aggregator.impl.CountByKeyAggregator;
import edu.snu.tempest.operator.window.aggregator.impl.KeyExtractor;
import edu.snu.tempest.operator.window.timescale.Timescale;
import edu.snu.tempest.operator.window.timescale.TimescaleParser;
import edu.snu.tempest.operator.window.timescale.parameter.CachingRate;
import edu.snu.tempest.operator.window.timescale.parameter.StartTime;
import edu.snu.tempest.operator.window.timescale.parameter.TimescaleString;
import edu.snu.tempest.test.util.IntegerExtractor;
import edu.snu.tempest.test.util.MTSTestUtils;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ComputationReuserTest {

  List<Timescale> timescales;
  Timescale ts1;
  Timescale ts2;
  Timescale ts3;
  long startTime;

  @Before
  public void initialize() {
    timescales = new LinkedList<>();
    ts1 = new Timescale(4, 2);
    ts2 = new Timescale(6, 3);
    ts3 = new Timescale(10, 4);
    timescales.add(ts1); timescales.add(ts2); timescales.add(ts3);
    startTime = 0;
  }

  @Test
  public void dynamicComputationReuserFinalAggregateTest() throws InjectionException {
    computationReuserTest(dynamicComputationReuserConfig());
  }

  @Test
  public void staticComputationReuserFinalAggregateTest() throws InjectionException {
    computationReuserTest(staticComputationReuserConfig());
  }


  @Test
  public void staticComputationReuserNextSliceTimeTest() throws InjectionException {
    nextSliceTimeTest(dynamicComputationReuserConfig());
  }

  @Test
  public void dynamiComputationReusercNextSliceTimeTest() throws InjectionException {
    nextSliceTimeTest(staticComputationReuserConfig());
  }

  @Test
  public void dynamicComputationReuserMultiThreadAggregationTest() throws InjectionException, InterruptedException {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindImplementation(CachingPolicy.class, CachingRatePolicy.class);
    jcb.bindImplementation(ComputationReuser.class, DynamicComputationReuser.class);
    multiThreadedFinalAggregation(jcb.build());
  }

  // TODO: #39 Need to improve static computation reuser for multiple threads.
  //@Test
  public void staticComputationReuserMultiThreadAggregationTest() throws InjectionException, InterruptedException {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindImplementation(ComputationReuser.class, StaticComputationReuser.class);
    multiThreadedFinalAggregation(jcb.build());
  }

  private Configuration dynamicComputationReuserConfig() {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(CachingRate.class, Integer.toString(1));
    jcb.bindImplementation(KeyExtractor.class, IntegerExtractor.class);
    jcb.bindNamedParameter(TimescaleString.class, TimescaleParser.parseToString(timescales));
    jcb.bindImplementation(CachingPolicy.class, CachingRatePolicy.class);
    jcb.bindNamedParameter(StartTime.class, Long.toString(startTime));
    jcb.bindImplementation(CAAggregator.class, CountByKeyAggregator.class);
    jcb.bindImplementation(Aggregator.class, CountByKeyAggregator.class);
    jcb.bindImplementation(ComputationReuser.class, DynamicComputationReuser.class);
    return jcb.build();
  }

  private Configuration staticComputationReuserConfig() {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindImplementation(KeyExtractor.class, IntegerExtractor.class);
    jcb.bindNamedParameter(TimescaleString.class, TimescaleParser.parseToString(timescales));
    jcb.bindNamedParameter(StartTime.class, Long.toString(startTime));
    jcb.bindImplementation(CAAggregator.class, CountByKeyAggregator.class);
    jcb.bindImplementation(Aggregator.class, CountByKeyAggregator.class);
    jcb.bindImplementation(ComputationReuser.class, StaticComputationReuser.class);
    return jcb.build();
  }

  /**
   * Test next slice time.
   */
  public void nextSliceTimeTest(final Configuration conf) throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector(conf);
    final ComputationReuser<Map<Integer, Long>> computationReuser =
        injector.getInstance(ComputationReuser.class);

    final long period = 12L;
    for (int i = 0; i < 5; i++) {
      Assert.assertEquals(2L + period * i, computationReuser.nextSliceTime());
      Assert.assertEquals(3L + period * i, computationReuser.nextSliceTime());
      Assert.assertEquals(4L + period * i, computationReuser.nextSliceTime());
      Assert.assertEquals(6L + period * i, computationReuser.nextSliceTime());
      Assert.assertEquals(8L + period * i, computationReuser.nextSliceTime());
      Assert.assertEquals(9L + period * i, computationReuser.nextSliceTime());
      Assert.assertEquals(10L + period * i, computationReuser.nextSliceTime());
      Assert.assertEquals(12L + period * i, computationReuser.nextSliceTime());
    }
  }

  /**
   * Multi-threaded final aggregation test.
   * This test runs final aggregation in multiple threads and checks the generated outputs
   */
  public void multiThreadedFinalAggregation(final Configuration conf)
      throws InjectionException, InterruptedException {
    final List<Timescale> timescales = new LinkedList<>();
    final Timescale ts1 = new Timescale(4, 2);
    final Timescale ts2 = new Timescale(8, 4);
    timescales.add(ts1);
    timescales.add(ts2);

    final ExecutorService executor = Executors.newFixedThreadPool(10);
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder(conf);
    jcb.bindNamedParameter(CachingRate.class, Integer.toString(1));
    jcb.bindImplementation(KeyExtractor.class, IntegerExtractor.class);
    jcb.bindNamedParameter(TimescaleString.class, TimescaleParser.parseToString(timescales));

    jcb.bindImplementation(CAAggregator.class, CountByKeyAggregator.class);
    jcb.bindImplementation(Aggregator.class, CountByKeyAggregator.class);
    jcb.bindNamedParameter(StartTime.class, Long.toString(startTime));

    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    // The computaton reuser does CountByKey operation for the timescales: [w=4,i=2] [w=8,i=4]
    final ComputationReuser<Map<Integer, Long>> computationReuser =
        injector.getInstance(ComputationReuser.class);

    // partial outputs for the computation reuser
    final Map<Integer, Long> partialOutput1 = new HashMap<>();
    partialOutput1.put(1, 10L); partialOutput1.put(2, 15L);

    final Map<Integer, Long> partialOutput2 = new HashMap<>();
    partialOutput2.put(3, 5L); partialOutput2.put(2, 15L);

    final Map<Integer, Long> partialOutput3 = new HashMap<>();
    partialOutput3.put(4, 10L); partialOutput3.put(3, 15L);

    final Map<Integer, Long> partialOutput4 = new HashMap<>();
    partialOutput4.put(5, 10L);


    // save partial results into computation reuser.
    computationReuser.savePartialOutput(0, 2, partialOutput1);
    final Queue<Boolean> check = new ConcurrentLinkedQueue<>();

    // multi-threaded final aggregation.
    // [0-2] final aggregation
    // it waits 2 seconds in order to delay the final aggregation
    executor.submit(new Runnable() {
      @Override
      public void run() {
        try {
          Thread.sleep(2000);
        } catch (final InterruptedException e) {
          e.printStackTrace();
        }
        final Map<Integer, Long> result1 = computationReuser.finalAggregate(-2, 2, ts1);
        check.add(partialOutput1.equals(result1));
      }
    });

    // [0-4] final aggregation
    // it waits 2 seconds in order to delay the final aggregation
    computationReuser.savePartialOutput(2, 4, partialOutput2);
    executor.submit(new Runnable() {
      @Override
      public void run() {
        try {
          Thread.sleep(2000);
        } catch (final InterruptedException e) {
          e.printStackTrace();
        }
        final Map<Integer, Long> result2 = computationReuser.finalAggregate(0, 4, ts1);
        check.add(MTSTestUtils.merge(partialOutput1, partialOutput2).equals(result2));
      }
    });

    // [0-4] final aggregation
    // it can reuse the above final aggregation. [0-2]
    // it should aggregate expected outputs even though the above final aggregation is delayed.
    executor.submit(new Runnable() {
      @Override
      public void run() {
        final Map<Integer, Long> result3 = computationReuser.finalAggregate(-4, 4, ts2);
        check.add(MTSTestUtils.merge(partialOutput1, partialOutput2).equals(result3));
      }
    });

    Thread.sleep(2000);
    // [2-6] final aggregation
    // it waits 2 seconds in order to delay the final aggregation
    computationReuser.savePartialOutput(4, 6, partialOutput3);
    executor.submit(new Runnable() {
      @Override
      public void run() {
        try {
          Thread.sleep(2000);
        } catch (final InterruptedException e) {
          e.printStackTrace();
        }
        final Map<Integer, Long> result4 = computationReuser.finalAggregate(2, 6, ts1);
        check.add(MTSTestUtils.merge(partialOutput2, partialOutput3).equals(result4));
      }
    });

    Thread.sleep(2000);
    // [4-8] final aggregation
    // it waits 2 seconds in order to delay the final aggregation
    computationReuser.savePartialOutput(6, 8, partialOutput4);
    executor.submit(new Runnable() {
      @Override
      public void run() {
        try {
          Thread.sleep(2000);
        } catch (final InterruptedException e) {
          e.printStackTrace();
        }
        final Map<Integer, Long> result5 = computationReuser.finalAggregate(4, 8, ts1);
        check.add(MTSTestUtils.merge(partialOutput3, partialOutput4).equals(result5));
      }
    });

    // [0-8] final aggregation
    // it can reuse the above final aggregation. ([0-4], [4-8] <- it is delayed)
    // it should aggregate expected outputs even though the above final aggregation is delayed.
    executor.submit(new Runnable() {
      @Override
      public void run() {
        final Map<Integer, Long> result6 = computationReuser.finalAggregate(0, 8, ts2);
        check.add(MTSTestUtils.merge(partialOutput1, partialOutput2,
            partialOutput3, partialOutput4).equals(result6));
      }
    });
    executor.shutdown();
    executor.awaitTermination(10, TimeUnit.SECONDS);

    for (final boolean c : check) {
      assert(c);
    }
  }

  /**
   * test computation reuser final aggregation.
   */
  public void computationReuserTest(final Configuration conf) throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector(conf);

    final ComputationReuser<Map<Integer, Long>> computationReuser =
        injector.getInstance(ComputationReuser.class);

    final Map<Integer, Long> partialOutput1 = new HashMap<>();
    partialOutput1.put(1, 10L); partialOutput1.put(2, 15L);

    final Map<Integer, Long> partialOutput2 = new HashMap<>();
    partialOutput2.put(3, 5L); partialOutput2.put(2, 15L);

    final Map<Integer, Long> partialOutput3 = new HashMap<>();
    partialOutput3.put(4, 10L); partialOutput3.put(3, 15L);

    final Map<Integer, Long> partialOutput4 = new HashMap<>();
    partialOutput4.put(5, 10L);

    final Map<Integer, Long> partialOutput5 = new HashMap<>();
    partialOutput5.put(4, 10L);

    final Map<Integer, Long> partialOutput6 = new HashMap<>();
    partialOutput6.put(2, 10L);

    final Map<Integer, Long> partialOutput7 = new HashMap<>();
    partialOutput7.put(3, 10L);

    final Map<Integer, Long> partialOutput8 = new HashMap<>();
    partialOutput8.put(5, 10L);

    final Map<Integer, Long> partialOutput9 = new HashMap<>();
    partialOutput9.put(3, 10L);

    final Map<Integer, Long> partialOutput10 = new HashMap<>();
    partialOutput10.put(2, 10L);

    final Map<Integer, Long> partialOutput11 = new HashMap<>();
    partialOutput11.put(3, 10L);

    computationReuser.savePartialOutput(0, 2, partialOutput1);
    // [w=4, i=2] final aggregation at time 2
    final Map<Integer, Long> result1 = computationReuser.finalAggregate(-2, 2, ts1);
    Assert.assertEquals(partialOutput1, result1);

    computationReuser.savePartialOutput(2, 3, partialOutput2);
    // [w=6, i=3] final aggregation at time 3
    final Map<Integer, Long> result2 = computationReuser.finalAggregate(-3, 3, ts2);
    Assert.assertEquals(MTSTestUtils.merge(partialOutput1, partialOutput2), result2);

    computationReuser.savePartialOutput(3, 4, partialOutput3);
    // [w=4, i=2] final aggregation at time 4
    final Map<Integer, Long> result3 = computationReuser.finalAggregate(0, 4, ts1);
    Assert.assertEquals(MTSTestUtils.merge(partialOutput1, partialOutput2, partialOutput3), result3);
    // [w=10, i=4] final aggregation at time 4
    final Map<Integer, Long> result4 = computationReuser.finalAggregate(-6, 4, ts3);
    Assert.assertEquals(MTSTestUtils.merge(partialOutput1, partialOutput2, partialOutput3), result4);

    computationReuser.savePartialOutput(4, 6, partialOutput4);
    // [w=4, i=2] final aggregation at time 6
    final Map<Integer, Long> result5 = computationReuser.finalAggregate(2, 6, ts1);
    Assert.assertEquals(MTSTestUtils.merge(partialOutput2, partialOutput3, partialOutput4), result5);
    // [w=6, i=3] final aggregation at time 6
    final Map<Integer, Long> result6 = computationReuser.finalAggregate(0, 6, ts2);
    Assert.assertEquals(MTSTestUtils.merge(partialOutput1,
        partialOutput2, partialOutput3, partialOutput4), result6);

    computationReuser.savePartialOutput(6, 8, partialOutput5);
    // [w=4, i=2] final aggregation at time 8
    final Map<Integer, Long> result7 = computationReuser.finalAggregate(4, 8, ts1);
    Assert.assertEquals(MTSTestUtils.merge(partialOutput4, partialOutput5), result7);
    // [w=10, i=4] final aggregation at time 8
    final Map<Integer, Long> result8 = computationReuser.finalAggregate(-2, 8, ts3);
    Assert.assertEquals(MTSTestUtils.merge(partialOutput1,
        partialOutput2, partialOutput3, partialOutput4, partialOutput5), result8);

    computationReuser.savePartialOutput(8, 9, partialOutput6);
    // [w=6, i=3] final aggregation at time 9
    final Map<Integer, Long> result9 = computationReuser.finalAggregate(3, 9, ts2);
    Assert.assertEquals(MTSTestUtils.merge(partialOutput3, partialOutput4,
        partialOutput5, partialOutput6), result9);

    computationReuser.savePartialOutput(9, 10, partialOutput7);
    // [w=4, i=2] final aggregation at time 10
    final Map<Integer, Long> result10 = computationReuser.finalAggregate(6, 10, ts1);
    Assert.assertEquals(MTSTestUtils.merge(partialOutput5, partialOutput6, partialOutput7), result10);

    computationReuser.savePartialOutput(10, 12, partialOutput8);
    // [w=4, i=2] final aggregation at time 12
    final Map<Integer, Long> result11 = computationReuser.finalAggregate(8, 12, ts1);
    Assert.assertEquals(MTSTestUtils.merge(partialOutput6, partialOutput7, partialOutput8), result11);
    // [w=6, i=3] final aggregation at time 12
    final Map<Integer, Long> result12 = computationReuser.finalAggregate(6, 12, ts2);
    Assert.assertEquals(MTSTestUtils.merge(partialOutput5,
        partialOutput6, partialOutput7, partialOutput8), result12);
    // [w=10, i=4] final aggregation at time 12
    final Map<Integer, Long> result13 = computationReuser.finalAggregate(2, 12, ts3);
    Assert.assertEquals(MTSTestUtils.merge(partialOutput2,
        partialOutput3, partialOutput4, partialOutput5,
        partialOutput6, partialOutput7, partialOutput8), result13);

    computationReuser.savePartialOutput(12, 14, partialOutput9);
    // [w=4, i=2] final aggregation at time 14
    final Map<Integer, Long> result14 = computationReuser.finalAggregate(10, 14, ts1);
    Assert.assertEquals(MTSTestUtils.merge(partialOutput8, partialOutput9), result14);

    computationReuser.savePartialOutput(14, 15, partialOutput10);
    // [w=6, i=3] final aggregation at time 15
    final Map<Integer, Long> result15 = computationReuser.finalAggregate(9, 15, ts2);
    Assert.assertEquals(MTSTestUtils.merge(partialOutput7, partialOutput8,
        partialOutput9, partialOutput10), result15);

    computationReuser.savePartialOutput(15, 16, partialOutput11);
    // [w=4, i=2] final aggregation at time 16
    final Map<Integer, Long> result16 = computationReuser.finalAggregate(12, 16, ts1);
    Assert.assertEquals(MTSTestUtils.merge(partialOutput9, partialOutput10, partialOutput11), result16);
    // [w=10, i=4] final aggregation at time 16
    final Map<Integer, Long> result17 = computationReuser.finalAggregate(6, 16, ts3);
    Assert.assertEquals(MTSTestUtils.merge(partialOutput5, partialOutput6, partialOutput7,
        partialOutput8, partialOutput9, partialOutput10, partialOutput11), result17);
  }
}
