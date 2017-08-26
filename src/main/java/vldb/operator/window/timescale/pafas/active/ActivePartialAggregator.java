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
package vldb.operator.window.timescale.pafas.active;

import org.apache.reef.tang.annotations.Parameter;
import vldb.evaluation.Metrics;
import vldb.operator.window.aggregator.CAAggregator;
import vldb.operator.window.timescale.TimeMonitor;
import vldb.operator.window.timescale.common.PartialAggregator;
import vldb.operator.window.timescale.common.SpanTracker;
import vldb.operator.window.timescale.common.Timespan;
import vldb.operator.window.timescale.pafas.event.WindowTimeEvent;
import vldb.operator.window.timescale.parameter.StartTime;

import javax.inject.Inject;
import java.util.List;
import java.util.logging.Logger;

/**
 * This executes incremental aggregation and slices the results.
 * @param <I> input
 * @param <V> aggregated result
 */
public final class ActivePartialAggregator<I, V> implements PartialAggregator<I> {
  private static final Logger LOG = Logger.getLogger(ActivePartialAggregator.class.getName());

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

  private final ActiveFinalAggregator<V> finalAggregator;

  private long nextRealTime;

  private final Metrics metrics;

  private final TimeMonitor timeMonitor;

  private final ActivePartialTimespans activePartialTimespans;

  /**
   * DefaultSlicedWindowOperatorImpl.
   * @param aggregator an aggregator for incremental aggregation
   * @param startTime a start time of the mts operator
   */
  @Inject
  private ActivePartialAggregator(
      final CAAggregator<I, V> aggregator,
      final SpanTracker<V> spanTracker,
      final ActiveFinalAggregator<V> finalAggregator,
      final Metrics metrics,
      @Parameter(StartTime.class) final Long startTime,
      final ActivePartialTimespans activePartialTimespans,
      final TimeMonitor timeMonitor) {
    this.timeMonitor = timeMonitor;
    this.activePartialTimespans = activePartialTimespans;
    this.metrics = metrics;
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
    if (val instanceof WindowTimeEvent) {
      final long tickTime = ((WindowTimeEvent) val).time;
      if (activePartialTimespans.isActive(tickTime)) {
        // Trigger final aggregator without storing!
        long stt = System.nanoTime();
        final List<Timespan> finalTimespans = spanTracker.getFinalTimespans(tickTime);
        finalAggregator.triggerFinalAggregation(finalTimespans, bucket);
        long ett = System.nanoTime();
        timeMonitor.finalTime += (ett - stt);
      }

      if (isSliceTime(tickTime)) {
        final V partialAggregation = bucket;
        bucket = aggregator.init();
        //System.out.println("PARTIAL_SIZE: " + ((Map)partialAggregation).size() + "\t" + (prevSliceTime) + "-" + (nextSliceTime));
        final long next = nextSliceTime;
        final long prev = prevSliceTime;
        prevSliceTime = nextSliceTime;
        nextSliceTime = spanTracker.getNextSliceTime(prevSliceTime);
        spanTracker.putAggregate(partialAggregation, new Timespan(prev, next, null));

        long stt = System.nanoTime();

        final List<Timespan> finalTimespans = spanTracker.getFinalTimespans(tickTime);
        finalAggregator.triggerFinalAggregation(finalTimespans);

        long ett = System.nanoTime();
        timeMonitor.finalTime += (ett - stt);
      }
    } else {
      final long st = System.nanoTime();
      aggregator.incrementalAggregate(bucket, val);
      final long et = System.nanoTime();
      timeMonitor.partialTime += (et - st);
      metrics.incrementPartial();
    }
  }

  @Override
  public void close() throws Exception {
  }
}