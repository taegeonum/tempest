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
import vldb.operator.window.timescale.profiler.AggregationCounter;

import javax.inject.Inject;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This executes incremental aggregation and slices the results.
 * @param <I> input
 * @param <V> aggregated result
 */
public final class CountBasedPartialAggregator<I, V> implements PartialAggregator<I> {
  private static final Logger LOG = Logger.getLogger(CountBasedPartialAggregator.class.getName());

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

  private final SpanTracker<V> spanTracker;

  private final FinalAggregator<V> finalAggregator;

  private final AggregationCounter aggregationCounter;

  private final ExecutorService executor = Executors.newSingleThreadExecutor();

  private long count = 0;
  /**
   * DefaultSlicedWindowOperatorImpl.
   * @param aggregator an aggregator for incremental aggregation
   * @param startTime a start time of the mts operator
   */
  @Inject
  private CountBasedPartialAggregator(
      final CAAggregator<I, V> aggregator,
      final SpanTracker<V> spanTracker,
      final FinalAggregator<V> finalAggregator,
      final AggregationCounter aggregationCounter,
      @Parameter(StartTime.class) final Long startTime) {
    this.aggregationCounter = aggregationCounter;
    this.aggregator = aggregator;
    this.bucket = aggregator.init();
    this.prevSliceTime = startTime;
    this.spanTracker = spanTracker;
    this.finalAggregator = finalAggregator;
    this.nextSliceTime = spanTracker.getNextSliceTime(startTime);
  }

  /**
   * Aggregates input into the current bucket.
   * @param val input
   */
  @Override
  public void execute(final I val) {
    LOG.log(Level.FINE, "SlicedWindow aggregates input of [" +  val + "]");
    aggregator.incrementalAggregate(bucket, val);
    aggregationCounter.incrementPartialAggregation();
    count += 1;
    if (count == (nextSliceTime - prevSliceTime) * 100) {
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
      count = 0;
    }
  }

  @Override
  public void close() throws Exception {
    executor.shutdown();
  }
}