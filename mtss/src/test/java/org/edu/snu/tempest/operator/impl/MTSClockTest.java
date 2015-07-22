package org.edu.snu.tempest.operator.impl;

import org.edu.snu.tempest.Timescale;
import org.edu.snu.tempest.operator.Clock;
import org.edu.snu.tempest.operator.OverlappingWindowOperator;
import org.edu.snu.tempest.operator.SlicedWindowOperator;
import org.edu.snu.tempest.operator.Subscription;
import org.edu.snu.tempest.utils.Monitor;
import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.mock;

public class MTSClockTest {

  @Test
  public void incrementTimeTest() throws Exception {
    Monitor monitor = new Monitor();
    Queue<Long> queue = new LinkedList<>();
    long startTime = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime());
    SlicedWindowOperator<Integer> operator = new TestSlicedWindowOperator(monitor, queue, startTime);

    Clock clock = new DefaultMTSClockImpl(operator);
    clock.start();
    monitor.mwait();
    
    Assert.assertEquals(3, queue.size());
    Long time1 = queue.poll();
    Long time2 = queue.poll();
    Long time3 = queue.poll();
    
    Assert.assertEquals(1L, time2 - time1);
    Assert.assertEquals(1L, time3 - time2);

    clock.close();
  }
  
  @Test
  public void overlappingWindowSubscribeUnsubscribeTest() throws Exception {
    AtomicInteger counter = new AtomicInteger();
    OverlappingWindowOperator<Map<Integer, Integer>> operator = new TestOverlappingWindowOperator(counter);
    Clock clock = new DefaultMTSClockImpl(mock(SlicedWindowOperator.class));
    Subscription<Timescale> subscription = clock.subscribe(operator);
    Thread.sleep(3000);
    subscription.unsubscribe();
    int prevCounter = counter.get();
    
    Thread.sleep(2000);
    Assert.assertEquals(prevCounter, counter.get());
    clock.close();
  }
  
  @Test
  public void multipleOverlappingWindowSubscriptionTest() throws Exception {
    //TODO 
  }
  
  /*
   * It should call SlicedWindowOperator.onNext first
   */
  @Test
  public void notificationSchedulingTest() throws Exception {
    Queue<String> queue = new ConcurrentLinkedQueue<>();
    Monitor monitor = new Monitor();
    SlicedWindowOperator<Integer> operator = new TestSlicedWindowOperator2(monitor, queue);
    OverlappingWindowOperator<Map<Integer, Integer>> ow1 = new TestOverlappingWindowOperator2(queue);
    OverlappingWindowOperator<Map<Integer, Integer>> ow2 = new TestOverlappingWindowOperator2(queue);
    
    Clock clock = new DefaultMTSClockImpl(operator);
    clock.subscribe(ow1);
    clock.subscribe(ow2);
    clock.start();
    
    monitor.mwait();
    clock.close();
    
    int i = 0;
    for (String val : queue) {
      if ((i % 3) == 0) {
        Assert.assertEquals("SlicedWindow", val);
      } else {
        Assert.assertEquals("OverlappingWindow", val);
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
      queue.add(time);
      System.out.println("Time: " + time);
      if (count == 2) {
        monitor.mnotify();
      }
      
      count++;
    }

    @Override
    public void execute(Integer val) {}

    @Override
    public void onTimescaleAddition(Timescale ts, final long t) {}

    @Override
    public void onTimescaleDeletion(Timescale ts) {}
    
  }
  
  class TestOverlappingWindowOperator implements OverlappingWindowOperator<Map<Integer, Integer>> {

    private final AtomicInteger counter;
    
    public TestOverlappingWindowOperator(final AtomicInteger counter) {
      this.counter = counter;
    }
    
    @Override
    public void onNext(Long paramT) {
      counter.incrementAndGet();
      System.out.println("Time: " + paramT);
    }

    @Override
    public Timescale getTimescale() {
      return null;
    }
    
  }
  
  class TestSlicedWindowOperator2 implements SlicedWindowOperator<Integer> {
    private int count = 0;
    private final Monitor monitor;
    private final Queue<String> queue;
    
    public TestSlicedWindowOperator2( final Monitor monitor, final Queue<String> queue) {
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

    @Override
    public void onTimescaleAddition(Timescale ts, long t) {}
    
    @Override
    public void onTimescaleDeletion(Timescale ts) {}
    
  }
  
  class TestOverlappingWindowOperator2 implements OverlappingWindowOperator<Map<Integer, Integer>> {

    private final Queue<String> queue;
    
    public TestOverlappingWindowOperator2(final Queue<String> queue) {
      this.queue = queue;
    }
    
    @Override
    public void onNext(Long paramT) {
      queue.add("OverlappingWindow");
    }

    @Override
    public Timescale getTimescale() {
      return null;
    }
    
  }
}
