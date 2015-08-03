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
package edu.snu.tempest.operator.window.time.mts.impl;

import edu.snu.tempest.operator.common.DefaultSubscription;
import edu.snu.tempest.operator.common.Subscription;
import edu.snu.tempest.operator.window.time.Timescale;
import edu.snu.tempest.operator.window.time.common.OverlappingWindowOperator;
import org.apache.reef.wake.impl.DefaultThreadFactory;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Default MTS operator scheduler implementation.
 * It triggers a sliced window operator and overlapping window operators every second.
 *
 * It first executes a SlicedWindowOperator.
 * After that, it executes OverlappingWindowOperators asc ordered by window size.
 */
final class MTSOperatorScheduler {

  private static final Logger LOG = Logger.getLogger(MTSOperatorScheduler.class.getName());

  /**
   * Overlapping window operators.
   */
  private final PriorityQueue<OverlappingWindowOperator<?>> handlers;

  /**
   * A sliced window operator.
   */
  private final SlicedWindowOperator<?> slicedWindowOperator;

  /**
   * SchedulerExecutor for a sliced window operator.
   * This is a thread for slicing the aggregated input.
   * In MTS Scheduler, It triggers a sliced window operator every second, by sending current time.
   * It calls the `slicedWindowOperator.onNext(current_time)` to slice the aggregated input.
   * In SlicedWindowOperator, if the current time is equal to the next slice time,
   * then it slices the aggregated input and creates a new bucket for next partial aggregation.
   * After slicing the aggregated input,
   * It (in sliced window operator) saves the result into ComputationReuser,
   * by calling `computationReuser.savePartialOutput`
   */
  private final ScheduledExecutorService scheduler;

  /**
   * It is a thread for final aggregation.
   * After slicing the aggregated input in `scheduler`,
   * the `scheduler` executes this `executor` to compute final aggregation.
   * The `executor` calls overlapping window operators in the ascendant ordered by window size,
   * and it computes final aggregation if current time is equal to the interval of the overlapping window operator.
   */
  private final ExecutorService executor;

  /**
   * Tick time of the scheduler.
   */
  private final long tickTime;

  /**
   * started flag.
   */
  private final AtomicBoolean started = new AtomicBoolean(false);

  /**
   * Previous triggered time.
   */
  private long prevTime;

  public MTSOperatorScheduler(final SlicedWindowOperator<?> swo,
                              final long tickTime,
                              final TimeUnit tickTimeUnit) {
    this.handlers = new PriorityQueue<>(10, new OWOComparator());
    this.scheduler = Executors.newScheduledThreadPool(1,
        new DefaultThreadFactory("MTSOperatorScheduler"));
    this.executor = Executors.newFixedThreadPool(1);
    this.slicedWindowOperator = swo;
    this.tickTime = tickTimeUnit.toMillis(tickTime);
    this.prevTime = 0;
  }
  
  public MTSOperatorScheduler(final SlicedWindowOperator<?> swo) {
    this(swo, 200L, TimeUnit.MILLISECONDS);
  }

  /**
   * Triggers a sliced window operator and overlapping window operators every second.
   * It first triggers sliced window operator
   * because sliced window operator should save partial aggregation before final aggregation.
   * After that, it triggers overlapping window operators in order to trigger final aggregation.
   */
  public void start() {
    if (started.compareAndSet(false, true)) {
      LOG.log(Level.FINE, "MTSOperatorScheduler start");
      scheduler.scheduleAtFixedRate(new Runnable() {
        @Override
        public void run() {
          final long time = getCurrentTime();
          if (prevTime < time) {
            LOG.log(Level.FINE, "MTSOperatorScheduler tickTime: " + time);
            prevTime = time;

            // trigger slicedWindowOperator to slice data.
            if (slicedWindowOperator != null) {
              slicedWindowOperator.onNext(time);
            }

            // trigger overlapping window operator to do final aggregation.
            synchronized (handlers) {
              for (final OverlappingWindowOperator<?> handler : handlers) {
                executor.submit(new Runnable() {
                  @Override
                  public void run() {
                    handler.onNext(time);
                  }
                });
              }
            }
          }
        }
      }, tickTime, tickTime, TimeUnit.MILLISECONDS);
    }
  }

  /**
   * Subscribe overlapping window operators.
   * It triggers overlapping window operators every second.
   * @param o an overlapping window operator.
   */
  public Subscription<Timescale> subscribe(final OverlappingWindowOperator<?> o) {
    LOG.log(Level.INFO, "MTSOperatorScheduler subscribe OverlappingWindowOperator: " + o);
    synchronized (handlers) {
      handlers.add(o);
    }
    return new DefaultSubscription<>(handlers, o, o.getTimescale());
  }

  private long getCurrentTime() {
    return TimeUnit.NANOSECONDS.toSeconds(System.nanoTime());
  }

  public void close() {
    scheduler.shutdown();
    executor.shutdown();
  }

  /**
   * An overlapping window operator which has small size of window
   * is executed before another OWOs having large size of window.
   */
  public class OWOComparator implements Comparator<OverlappingWindowOperator> {
    @Override
    public int compare(OverlappingWindowOperator x, OverlappingWindowOperator y) {
      if (x.getTimescale().windowSize < y.getTimescale().windowSize) {
        return -1;
      } else if (x.getTimescale().windowSize > y.getTimescale().windowSize) {
        return 1;
      } else {
        return 0;
      }
    }
  }
}