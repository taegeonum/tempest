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

import edu.snu.tempest.operator.window.timescale.parameter.SlicedWindowTriggerPeriod;
import vldb.operator.window.timescale.parameter.StartTime;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.Stage;
import org.apache.reef.wake.WakeParameters;
import org.apache.reef.wake.impl.DefaultThreadFactory;
import org.apache.reef.wake.impl.StageManager;

import javax.inject.Inject;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Stage that slices partial aggregation and saves the sliced results into computation reuser.
 * Also, it triggers overlapping window operators for final aggregation.
 *
 * @param <I> input
 */
final class SlicingStage<I, V> implements Stage {
  private static final Logger LOG = Logger.getLogger(SlicingStage.class.getName());

  /**
   * Is this stage closed or not.
   */
  private final AtomicBoolean closed = new AtomicBoolean(false);

  /**
   * A scheduled executor.
   */
  private final ScheduledExecutorService executor;

  /**
   * Shutdown timeout.
   */
  private final long shutdownTimeout = WakeParameters.EXECUTOR_SHUTDOWN_TIMEOUT;

  /**
   * Previous triggered time.
   */
  private long prevTriggeredTime;

  /**
   * Constructs a slicing stage.
   *
   * @param partialAggregator a sliced window operator
   * @param period               a period in milli-seconds
   */
  @Inject
  private SlicingStage(final PartialAggregator<I, V> partialAggregator,
                       @Parameter(SlicedWindowTriggerPeriod.class) final long period,
                       @Parameter(StartTime.class) final long startTime) {
    this.executor = Executors.newScheduledThreadPool(1, new DefaultThreadFactory(SlicingStage.class.getName()));
    this.prevTriggeredTime = startTime;
    this.executor.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        final long time = getCurrentTime();
        // Do not trigger if current time was already triggered
        // This prevents duplicated trigger.
        if (prevTriggeredTime < time) {
          LOG.log(Level.FINE, SlicingStage.class.getName() + " trigger: " + time);
          for (long triggerTime = prevTriggeredTime + 1; triggerTime <= time; triggerTime++) {
            // slice partial aggregation and save the result into computation reuser.
            partialAggregator.slice(triggerTime);
          }
          prevTriggeredTime = time;
        }
      }
    }, period, period, TimeUnit.MILLISECONDS);
    StageManager.instance().register(this);
  }

  private long getCurrentTime() {
    return TimeUnit.NANOSECONDS.toSeconds(System.nanoTime());
  }

  /**
   * Closes resources.
   *
   * @throws Exception
   */
  @Override
  public void close() throws Exception {
    if (closed.compareAndSet(false, true)) {
      executor.shutdown();
      if (!executor.awaitTermination(shutdownTimeout, TimeUnit.MILLISECONDS)) {
        LOG.log(Level.WARNING, "Executor did not terminate in " + shutdownTimeout + "ms.");
        final List<Runnable> droppedRunnables = executor.shutdownNow();
        LOG.log(Level.WARNING, "Executor dropped " + droppedRunnables.size() + " tasks.");
      }
    }
  }
}