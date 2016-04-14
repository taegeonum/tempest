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

import edu.snu.tempest.operator.OutputEmitter;
import edu.snu.tempest.operator.window.aggregator.CAAggregator;
import edu.snu.tempest.operator.window.timescale.parameter.StartTime;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This executes incremental aggregation and slices the results.
 * @param <I> input
 * @param <V> aggregated result
 */
final class DefaultPartialAggregator<I, V> implements PartialAggregator<I, V> {
  private static final Logger LOG = Logger.getLogger(DefaultPartialAggregator.class.getName());

  /**
   * Aggregator for incremental aggregation.
   */
  private final CAAggregator<I, V> aggregator;

  private OutputEmitter<PartialTimeWindowOutput<V>> emitter;

  /**
   * Next slice time provider.
   */
  private final NextEdgeProvider nextEdgeProvider;

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

  /**
   * DefaultSlicedWindowOperatorImpl.
   * @param aggregator an aggregator for incremental aggregation
   * @param startTime a start time of the mts operator
   */
  @Inject
  private DefaultPartialAggregator(
      final CAAggregator<I, V> aggregator,
      final NextEdgeProvider nextEdgeProvider,
      @Parameter(StartTime.class) final Long startTime) {
    this.aggregator = aggregator;
    this.nextEdgeProvider = nextEdgeProvider;
    this.bucket = aggregator.init();
    prevSliceTime = startTime;
    nextSliceTime = nextEdgeProvider.nextSliceTime();
  }

  /**
   * It slices current aggregated results and send them to output emitter.
   * @param currTime current time
   */
  @Override
  public synchronized void slice(final long currTime) {
    while (nextSliceTime < currTime) {
      prevSliceTime = nextSliceTime;
      nextSliceTime = nextEdgeProvider.nextSliceTime();
    }
    LOG.log(Level.FINE, "SlicedWindow tickTime " + currTime + ", nextSlice: " + nextSliceTime);

    if (nextSliceTime == currTime) {
      LOG.log(Level.FINE, "Sliced : [" + prevSliceTime + "-" + currTime + "]");
      // create a new bucket
      synchronized (sync) {
        final V partialAggregation = bucket;
        bucket = aggregator.init();
        // emit partial result to emitter.
        emitter.emit(new PartialTimeWindowOutput<V>(prevSliceTime, nextSliceTime, partialAggregation));
      }
      prevSliceTime = nextSliceTime;
      nextSliceTime = nextEdgeProvider.nextSliceTime();
    }
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
  public void prepare(final OutputEmitter<PartialTimeWindowOutput<V>> outputEmitter) {
    emitter = outputEmitter;
  }
}