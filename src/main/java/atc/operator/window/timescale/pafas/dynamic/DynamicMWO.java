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
package atc.operator.window.timescale.pafas.dynamic;

import org.apache.reef.tang.annotations.Parameter;
import atc.operator.OutputEmitter;
import atc.operator.window.aggregator.CAAggregator;
import atc.operator.window.timescale.TimeMonitor;
import atc.operator.window.timescale.Timescale;
import atc.operator.window.timescale.TimescaleWindowOperator;
import atc.operator.window.timescale.TimescaleWindowOutput;
import atc.operator.window.timescale.common.FinalAggregator;
import atc.operator.window.timescale.common.Timespan;
import atc.operator.window.timescale.pafas.event.WindowTimeEvent;
import atc.operator.window.timescale.parameter.NumThreads;
import atc.operator.window.timescale.parameter.StartTime;
import atc.operator.window.timescale.profiler.AggregationCounter;

import javax.inject.Inject;
import java.util.List;
import java.util.logging.Logger;

/**
 * This executes incremental aggregation and slices the results.
 * @param <I> input
 * @param <V> aggregated result
 */
public final class DynamicMWO<I, V> implements TimescaleWindowOperator<I, V> {
  private static final Logger LOG = Logger.getLogger(DynamicMWO.class.getName());

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

  private final DynamicSpanTrackerImpl<I, V> spanTracker;

  private final FinalAggregator<V> finalAggregator;

  private long nextRealTime;

  private final AggregationCounter aggregationCounter;

  private final TimeMonitor timeMonitor;

  private final int numThreads;

  /**
   * DefaultSlicedWindowOperatorImpl.
   * @param aggregator an aggregator for incremental aggregation
   * @param startTime a start time of the mts operator
   */
  @Inject
  private DynamicMWO(
      final CAAggregator<I, V> aggregator,
      final DynamicSpanTrackerImpl<I, V> spanTracker,
      final FinalAggregator<V> finalAggregator,
      final AggregationCounter aggregationCounter,
      @Parameter(NumThreads.class) final int numThreads,
      @Parameter(StartTime.class) final Long startTime,
      final TimeMonitor timeMonitor) {
    this.aggregationCounter = aggregationCounter;
    this.aggregator = aggregator;
    this.numThreads = numThreads;
    this.bucket = initBucket();
    this.prevSliceTime = startTime;
    this.spanTracker = spanTracker;
    this.nextSliceTime = spanTracker.getNextSliceTime(startTime);
    this.finalAggregator = finalAggregator;
    this.timeMonitor = timeMonitor;
  }

  private boolean isSliceTime(final long time) {
    if (nextSliceTime == time) {
      return true;
    } else {
      return false;
    }
  }

  private V initBucket() {
    return aggregator.init();
  }

  private int hashing(final I val) {
    return Math.abs(val.hashCode()) %  numThreads;
  }

  /**
   * Aggregates input into the current bucket.
   * @param val input
   */
  @Override
  public void execute(final I val) {
    if (val instanceof WindowTimeEvent) {
      if (isSliceTime(((WindowTimeEvent) val).time)) {
        //System.out.println("SLICE: " + prevSliceTime + ", " + nextSliceTime);
        slice(prevSliceTime, nextSliceTime);
      }
    } else {
      final long st = System.nanoTime();
      aggregator.incrementalAggregate(bucket, val);
      final long et = System.nanoTime();
      timeMonitor.partialTime += (et - st);
      aggregationCounter.incrementPartialAggregation();
    }
  }

  private void slice(final long prev, final long next) {
    final V partialAggregation = bucket;
    bucket = initBucket();
    //System.out.println("PARTIAL_SIZE: " + ((Map)partialAggregation).size() + "\t" + (prevSliceTime) + "-" + (nextSliceTime));
    //System.out.println("SLICE: " + prev + ", " + next);
    prevSliceTime = next;
    nextSliceTime = spanTracker.getNextSliceTime(prevSliceTime);
    //System.out.println("SLICE changed: " + prevSliceTime + ", " + nextSliceTime);
    spanTracker.putAggregate(partialAggregation, new Timespan(prev, next, null));
    final List<Timespan> finalTimespans = spanTracker.getFinalTimespans(next);
    //System.out.println("final timespans at " + next  + ": " + finalTimespans);
    finalAggregator.triggerFinalAggregation(finalTimespans);
  }

  @Override
  public void prepare(final OutputEmitter<TimescaleWindowOutput<V>> outputEmitter) {

  }

  @Override
  public void close() throws Exception {
  }

  @Override
  public void addWindow(final Timescale window, final long time) {
    spanTracker.addSlidingWindow(window, time);
    // 1) Update next slice time
    //System.out.println("PREV SLICE: " + nextSliceTime + ", at " + time);
    if (prevSliceTime < time) {
      // slice here!
      slice(prevSliceTime, time);
    } else {
      nextSliceTime = spanTracker.getNextSliceTime(time);
    }
  }

  @Override
  public void removeWindow(final Timescale window, final long time) {
    spanTracker.removeSlidingWindow(window, time);
    // 2) Update next slice time
    nextSliceTime = spanTracker.getNextSliceTime(prevSliceTime);
    //System.out.println("prevslice : " + prevSliceTime + ", nextSlice: " + nextSliceTime);
  }
}