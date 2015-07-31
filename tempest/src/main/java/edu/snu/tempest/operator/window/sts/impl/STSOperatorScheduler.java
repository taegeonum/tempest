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
package edu.snu.tempest.operator.window.sts.impl;

import edu.snu.tempest.operator.window.common.OverlappingWindowOperator;
import edu.snu.tempest.operator.window.mts.impl.SlicedWindowOperator;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.impl.DefaultThreadFactory;

import javax.inject.Inject;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Default STS operator scheduler implementation.
 * It triggers a sliced window operator and a overlapping window operator every second.
 *
 * It first executes a SlicedWindowOperator.
 * After that, it executes an overlapping window operator.
 */
final class STSOperatorScheduler {

  private static final Logger LOG = Logger.getLogger(STSOperatorScheduler.class.getName());

  /**
   * Overlapping window operator.
   */
  private final OverlappingWindowOperator<?> owo;

  /**
   * A sliced window operator.
   */
  private final SlicedWindowOperator<?> slicedWindowOperator;

  /**
   * SchedulerExecutor for a sliced window operator.
   * This is a thread for slicing the aggregated input.
   */
  private final ScheduledExecutorService scheduler;

  /**
   * It is a thread for final aggregation.
   * After slicing the aggregated input in `scheduler`,
   * the `scheduler` executes this `executor` to compute final aggregation.
   * The `executor` calls an overlapping window operator
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

  @NamedParameter(doc = "tick time (ms)", default_value = "200")
  public static final class TickTime implements Name<Long> {}

  @Inject
  public STSOperatorScheduler(final SlicedWindowOperator<?> swo,
                              final OverlappingWindowOperator<?> owo,
                              @Parameter(TickTime.class) final long tickTime,
                              final TimeUnit tickTimeUnit) {
    this.owo = owo;
    this.scheduler = Executors.newScheduledThreadPool(1,
        new DefaultThreadFactory("STSOperatorScheduler"));
    this.executor = Executors.newFixedThreadPool(1);
    this.slicedWindowOperator = swo;
    this.tickTime = tickTimeUnit.toMillis(tickTime);
    this.prevTime = 0;
  }

  @Inject
  public STSOperatorScheduler(final SlicedWindowOperator<?> swo,
                              final OverlappingWindowOperator<?> owo) {
    this(swo, owo, 200L, TimeUnit.MILLISECONDS);
  }

  /**
   * Triggers a sliced window operator and an overlapping window operator every second.
   * It first triggers sliced window operator
   * because sliced window operator should save partial aggregation before final aggregation.
   * After that, it triggers an overlapping window operator in order to trigger final aggregation.
   */
  public void start() {
    if (started.compareAndSet(false, true)) {
      LOG.log(Level.INFO, "STSOperatorScheduler start");
      scheduler.scheduleAtFixedRate(new Runnable() {
        @Override
        public void run() {
          final long time = getCurrentTime();
          if (prevTime < time) {
            LOG.log(Level.FINE, "STSOperatorScheduler tickTime: " + time);
            prevTime = time;

            // trigger slicedWindowOperator to slice data.
            if (slicedWindowOperator != null) {
              slicedWindowOperator.onNext(time);
            }

            // trigger overlapping window operator to do final aggregation.
            executor.submit(new Runnable() {
                @Override
                public void run() {
                  owo.onNext(time);
                }
              });
          }
        }
      }, tickTime, tickTime, TimeUnit.MILLISECONDS);
    }
  }

  private long getCurrentTime() {
    return TimeUnit.NANOSECONDS.toSeconds(System.nanoTime());
  }

  public void close() {
    scheduler.shutdown();
    executor.shutdown();
  }
}