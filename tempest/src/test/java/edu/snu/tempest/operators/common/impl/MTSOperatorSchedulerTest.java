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
package edu.snu.tempest.operators.common.impl;

import edu.snu.tempest.operators.Timescale;
import edu.snu.tempest.operators.common.MTSOperatorScheduler;
import edu.snu.tempest.operators.common.OverlappingWindowOperator;
import edu.snu.tempest.operators.common.Subscription;
import edu.snu.tempest.operators.staticmts.SlicedWindowOperator;
import edu.snu.tempest.utils.Monitor;
import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.mock;

public final class MTSOperatorSchedulerTest {

  /**
   * Scheduler increments time every second.
   */
  @Test
  public void schedulerIncrementTimeTest() throws Exception {
    final Monitor monitor = new Monitor();
    final Queue<Long> queue = new LinkedList<>();
    final long startTime = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime());
    final SlicedWindowOperator<Integer> operator = new TestSlicedWindowOperator(monitor, queue, startTime);
    final AtomicInteger owoCount = new AtomicInteger(0);

    final MTSOperatorScheduler mtsScheduler = new DefaultMTSOperatorSchedulerImpl(operator);
    final OverlappingWindowOperator<Integer> owo = new OverlappingWindowOperator<Integer>() {
      @Override
      public Timescale getTimescale() {
        return new Timescale(5, 2);
      }

      @Override
      public void onNext(final Long time) {
        if (owoCount.get() < 3) {
          owoCount.incrementAndGet();
        }
      }
    };

    mtsScheduler.subscribe(owo);
    mtsScheduler.start();
    monitor.mwait();
    
    Assert.assertEquals(3, queue.size());
    final Long time1 = queue.poll();
    final Long time2 = queue.poll();
    final Long time3 = queue.poll();
    
    Assert.assertEquals(1L, time2 - time1);
    Assert.assertEquals(1L, time3 - time2);
    mtsScheduler.close();
    Assert.assertEquals(3, owoCount.get());
  }

  /**
   * Overlapping window subscription test in scheduler.
   */
  @Test
  public void overlappingWindowSubscribeUnsubscribeTest() throws Exception {
    final AtomicInteger counter = new AtomicInteger();
    final OverlappingWindowOperator<Map<Integer, Integer>> operator = new TestOverlappingWindowOperator(counter,
        new Timescale(10, 5));
    final MTSOperatorScheduler mtsScheduler = new DefaultMTSOperatorSchedulerImpl(mock(SlicedWindowOperator.class));
    final Subscription<Timescale> subscription = mtsScheduler.subscribe(operator);
    Thread.sleep(3000);
    subscription.unsubscribe();
    final int prevCounter = counter.get();
    
    Thread.sleep(2000);
    Assert.assertEquals(prevCounter, counter.get());
    mtsScheduler.close();
  }

  /**
   * Scheduler should call SlicedWindowOperator.onNext first.
   * and call a overlapping window operator which has small size of window.
   */
  @Test
  public void notificationSchedulingTest() throws Exception {
    final Queue<String> queue = new ConcurrentLinkedQueue<>();
    final Monitor monitor = new Monitor();
    final SlicedWindowOperator<Integer> operator = new TestSlicedWindowOperator2(monitor, queue);
    final OverlappingWindowOperator<Map<Integer, Integer>> ow1 = new TestOverlappingWindowOperator2(queue,
        new Timescale(5,1));
    final OverlappingWindowOperator<Map<Integer, Integer>> ow2 = new TestOverlappingWindowOperator2(queue,
        new Timescale(10,2));

    final MTSOperatorScheduler mtsScheduler = new DefaultMTSOperatorSchedulerImpl(operator);
    mtsScheduler.subscribe(ow2);
    mtsScheduler.subscribe(ow1);
    mtsScheduler.start();
    
    monitor.mwait();
    mtsScheduler.close();
    
    int i = 0;
    for (String val : queue) {
      if ((i % 3) == 0) {
        Assert.assertEquals("SlicedWindow", val);
      } else if ((i % 3) == 1) {
        Assert.assertEquals("OverlappingWindow5", val);
      } else {
        Assert.assertEquals("OverlappingWindow10", val);
      }
      
      i++;
    }
  }
  
  class TestSlicedWindowOperator implements SlicedWindowOperator<Integer> {
    private int count = 0;
    private final Monitor monitor;
    private final Queue<Long> queue;
    private final Long startTime;

    public TestSlicedWindowOperator(final Monitor monitor, final Queue<Long> queue,
                                    final Long startTime) {
      this.monitor = monitor;
      this.queue = queue;
      this.startTime = startTime;
    }
    
    @Override
    public void onNext(Long time) {
      if (count < 3) {
        queue.add(time);
      }

      if (count == 2) {
        monitor.mnotify();
      }
      count++;
    }

    @Override
    public void execute(Integer val) {}
  }
  
  class TestOverlappingWindowOperator implements OverlappingWindowOperator<Map<Integer, Integer>> {

    private final AtomicInteger counter;
    private final Timescale ts;

    public TestOverlappingWindowOperator(final AtomicInteger counter,
                                         final Timescale ts) {
      this.counter = counter;
      this.ts = ts;
    }
    
    @Override
    public void onNext(final Long paramT) {
      counter.incrementAndGet();
      System.out.println("Time: " + paramT);
    }

    @Override
    public Timescale getTimescale() {
      return ts;
    }
    
  }
  
  class TestSlicedWindowOperator2 implements SlicedWindowOperator<Integer> {
    private int count = 0;
    private final Monitor monitor;
    private final Queue<String> queue;

    public TestSlicedWindowOperator2(final Monitor monitor, final Queue<String> queue) {
      this.monitor = monitor;
      this.queue = queue;
    }
    
    @Override
    public void onNext(Long time) {
      queue.add("SlicedWindow");
      System.out.println("Time: " + time);
      if (count == 3) {
        monitor.mnotify();
      }
      count++;
    }

    @Override
    public void execute(Integer val) {}
    
  }
  
  class TestOverlappingWindowOperator2 implements OverlappingWindowOperator<Map<Integer, Integer>> {

    private final Queue<String> queue;
    private final Timescale ts;

    public TestOverlappingWindowOperator2(final Queue<String> queue,
                                          final Timescale ts) {
      this.queue = queue;
      this.ts = ts;
    }
    
    @Override
    public void onNext(final Long paramT) {
      queue.add("OverlappingWindow" + ts.windowSize);
    }

    @Override
    public Timescale getTimescale() {
      return ts;
    }
    
  }
}
