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
package edu.snu.tempest.operator.window.time.impl;

import edu.snu.tempest.operator.window.aggregator.CAAggregator;
import edu.snu.tempest.operator.window.time.parameter.StartTime;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This executes incremental aggregation and slices the results.
 * @param <I> input
 * @param <V> aggregated result
 */
final class DefaultSlicedWindowOperator<I, V> implements SlicedWindowOperator<I> {
  private static final Logger LOG = Logger.getLogger(DefaultSlicedWindowOperator.class.getName());

  /**
   * Aggregator for incremental aggregation.
   */
  private final CAAggregator<I, V> aggregator;

  /**
   * A computation reuser for saving partial results.
   */
  private final ComputationReuser<V> computationReuser;

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
   * DynamicSlicedWindowOperatorImpl.
   * @param aggregator an aggregator for incremental aggregation
   * @param computationReuser a computation reuser for partial results.
   * @param startTime a start time of the mts operator
   */
  @Inject
  private DefaultSlicedWindowOperator(
      final CAAggregator<I, V> aggregator,
      final ComputationReuser<V> computationReuser,
      @Parameter(StartTime.class) final Long startTime) {
    this.aggregator = aggregator;
    this.computationReuser = computationReuser;
    this.bucket = aggregator.init();
    prevSliceTime = startTime;
    nextSliceTime = computationReuser.nextSliceTime();
  }

  /**
   * It creates a new bucket every next slice time to slice the incrementally aggregated data.
   * @param currTime current time
   */
  @Override
  public synchronized void onNext(final Long currTime) {
    while (nextSliceTime < currTime) {
      prevSliceTime = nextSliceTime;
      nextSliceTime = computationReuser.nextSliceTime();
    }
    LOG.log(Level.FINE, "SlicedWindow tickTime " + currTime + ", nextSlice: " + nextSliceTime);

    if (nextSliceTime == currTime) {
      LOG.log(Level.FINE, "Sliced : [" + prevSliceTime + "-" + currTime + "]");
      // create a new bucket
      synchronized (sync) {
        final V partialAggregation = bucket;
        bucket = aggregator.init();
        // saves output to computation reuser
        computationReuser.savePartialOutput(prevSliceTime, nextSliceTime, partialAggregation);
      }
      prevSliceTime = nextSliceTime;
      nextSliceTime = computationReuser.nextSliceTime();
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

}
