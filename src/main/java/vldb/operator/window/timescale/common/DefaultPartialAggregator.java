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
import vldb.operator.window.timescale.TimeMonitor;
import vldb.operator.window.timescale.pafas.event.WindowTimeEvent;
import vldb.operator.window.timescale.parameter.StartTime;
import vldb.operator.window.timescale.profiler.AggregationCounter;

import javax.inject.Inject;
import java.util.List;
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

  private final SpanTracker<V> spanTracker;

  private final FinalAggregator<V> finalAggregator;

  private long nextRealTime;

  private final AggregationCounter aggregationCounter;

  private final TimeMonitor timeMonitor;

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
      final AggregationCounter aggregationCounter,
      @Parameter(StartTime.class) final Long startTime,
      final TimeMonitor timeMonitor) {
    this.timeMonitor = timeMonitor;
    this.aggregationCounter = aggregationCounter;
    this.aggregator = aggregator;
    this.bucket = aggregator.init();
    this.prevSliceTime = startTime;
    this.spanTracker = spanTracker;
    this.finalAggregator = finalAggregator;
    this.nextSliceTime = spanTracker.getNextSliceTime(startTime);
    this.nextRealTime = System.currentTimeMillis() + (nextSliceTime - prevSliceTime) * 1000;
  }

  private boolean isSliceTime(final long time) {
    if (nextSliceTime == time) {
      return true;
    } else {
      return false;
    }
  }

  /**
   * Aggregates input into the current bucket.
   * @param val input
   */
  @Override
  public void execute(final I val) {
    LOG.log(Level.FINE, "SlicedWindow aggregates input of [" +  val + "]");
    if (val instanceof WindowTimeEvent) {
      if (isSliceTime(((WindowTimeEvent) val).time)) {
        final V partialAggregation = bucket;
        bucket = aggregator.init();
        //System.out.println("PARTIAL_SIZE: " + ((Map)partialAggregation).size() + "\t" + (prevSliceTime) + "-" + (nextSliceTime));
        final long next = nextSliceTime;
        final long prev = prevSliceTime;
        prevSliceTime = nextSliceTime;
        nextSliceTime = spanTracker.getNextSliceTime(prevSliceTime);
        spanTracker.putAggregate(partialAggregation, new Timespan(prev, next, null));
        final List<Timespan> finalTimespans = spanTracker.getFinalTimespans(next);
        finalAggregator.triggerFinalAggregation(finalTimespans);
      }
    } else {
      final long st = System.nanoTime();
      aggregator.incrementalAggregate(bucket, val);
      final long et = System.nanoTime();
      timeMonitor.partialTime += (et - st);
      aggregationCounter.incrementPartialAggregation();
    }
  }

  @Override
  public void close() throws Exception {
  }
}