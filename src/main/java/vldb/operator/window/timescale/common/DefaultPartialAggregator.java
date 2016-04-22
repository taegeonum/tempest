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
package vldb.operator.window.timescale.common;

import org.apache.reef.tang.annotations.Parameter;
import vldb.operator.window.aggregator.CAAggregator;
import vldb.operator.window.timescale.parameter.StartTime;

import javax.inject.Inject;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This executes incremental aggregation and slices the results.
 * @param <I> input
 * @param <V> aggregated result
 */
final class DefaultPartialAggregator<I, V> implements PartialAggregator<I> {
  private static final Logger LOG = Logger.getLogger(DefaultPartialAggregator.class.getName());

  /**
   * Aggregator for incremental aggregation.
   */
  private final CAAggregator<I, V> aggregator;

  /**
   * Previous slice time.
   */
  private long prevSliceTime;

  /**
   * Current slice time.
   */
  private long nextSliceTime;

  /**
   * A bucket for incremental aggregation.
   * It saves aggregated results for partial aggregation.
   */
  private V bucket;

  /**
   * Sync object for bucket.
   */
  private final Object sync = new Object();

  private final ScheduledExecutorService executorService;

  private final SpanTracker<V> spanTracker;

  private final FinalAggregator<V> finalAggregator;

  /**
   * DefaultSlicedWindowOperatorImpl.
   * @param aggregator an aggregator for incremental aggregation
   * @param startTime a start time of the mts operator
   */
  @Inject
  private DefaultPartialAggregator(
      final CAAggregator<I, V> aggregator,
      final SpanTracker<V> spanTracker,
      final FinalAggregator<V> finalAggregator,
      @Parameter(StartTime.class) final Long startTime) {
    this.aggregator = aggregator;
    this.bucket = aggregator.init();
    this.executorService = Executors.newSingleThreadScheduledExecutor();
    this.prevSliceTime = startTime;
    this.spanTracker = spanTracker;
    this.finalAggregator = finalAggregator;
    this.nextSliceTime = spanTracker.getNextSliceTime(startTime);
    executorService.schedule(new PartialAggregateEmitter(), nextSliceTime - prevSliceTime, TimeUnit.SECONDS);
  }

  /**
   * Aggregates input into the current bucket.
   * @param val input
   */
  @Override
  public void execute(final I val) {
    LOG.log(Level.FINE, "SlicedWindow aggregates input of [" +  val + "]");
    synchronized (sync) {
      aggregator.incrementalAggregate(bucket, val);
    }
  }

  @Override
  public void close() throws Exception {
    executorService.shutdown();
  }

  final class PartialAggregateEmitter implements Runnable {
    @Override
    public void run() {
      // create a new bucket
      V partialAggregation = null;
      synchronized (sync) {
        partialAggregation = bucket;
        bucket = aggregator.init();
      }
      // Store the partial aggregate to SpanTracker.
      //System.out.println("PUT: [" + prevSliceTime + ", " + nextSliceTime + ")");
      spanTracker.putAggregate(partialAggregation, new Timespan(prevSliceTime, nextSliceTime, null));
      // Get final timespans
      final List<Timespan> finalTimespans = spanTracker.getFinalTimespans(nextSliceTime);
      finalAggregator.triggerFinalAggregation(finalTimespans);
      prevSliceTime = nextSliceTime;
      nextSliceTime = spanTracker.getNextSliceTime(prevSliceTime);
      executorService.schedule(new PartialAggregateEmitter(), nextSliceTime - prevSliceTime, TimeUnit.SECONDS);
    }
  }
}