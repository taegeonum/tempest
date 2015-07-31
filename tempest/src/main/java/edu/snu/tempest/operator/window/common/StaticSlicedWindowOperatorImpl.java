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
package edu.snu.tempest.operator.window.common;


import edu.snu.tempest.operator.window.Aggregator;
import edu.snu.tempest.operator.window.mts.impl.SlicedWindowOperator;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This implementation is based on "On-the-fly Sharing for Streamed Aggregation" paper
 * It chops input stream into paired sliced window.
 */
public final class StaticSlicedWindowOperatorImpl<I, V> implements SlicedWindowOperator<I> {

  private static final Logger LOG = Logger.getLogger(StaticSlicedWindowOperatorImpl.class.getName());

  /**
   * Aggregator for partial aggregation.
   */
  private final Aggregator<I, V> aggregator;

  /**
   * An output generator for creating window outputs.
   */
  private final StaticTSOutputGenerator<V> tsOutputGenerator;

  /**
   * The next slice time to be sliced.
   */
  private long nextSliceTime;

  /**
   * The previous slice time.
   */
  private long prevSliceTime;

  /**
   * Sync object for the bucket.
   */
  private final Object sync = new Object();

  /**
   * A bucket for partial aggregation.
   */
  private V bucket;

  /**
   * StaticSlicedWindowOperatorImpl.
   * @param aggregator an aggregator for partial aggregation
   * @param tsOutputGenerator an output generator for creating window outputs.
   * @param startTime a start time of the mts operator
   */
  @Inject
  public StaticSlicedWindowOperatorImpl(
      final Aggregator<I, V> aggregator,
      final StaticTSOutputGenerator<V> tsOutputGenerator,
      final long startTime) {
    this.aggregator = aggregator;
    this.tsOutputGenerator = tsOutputGenerator;
    this.prevSliceTime = startTime;
    this.nextSliceTime = tsOutputGenerator.nextSliceTime();
    this.bucket = aggregator.init();
  }

  /**
   * Slice partial aggregation and save the partial aggregation into tsOutputGenerator in order to reuse it.
   * @param currTime current time
   */
  @Override
  public synchronized void onNext(final Long currTime) {
    LOG.log(Level.FINE, "SlicedWindow tickTime " + currTime + ", nextSlice: " + nextSliceTime);
    while (nextSliceTime < currTime) {
      prevSliceTime = nextSliceTime;
      nextSliceTime = tsOutputGenerator.nextSliceTime();
    }

    if (nextSliceTime == currTime) {
      LOG.log(Level.FINE, "Sliced : [" + prevSliceTime + "-" + currTime + "]");
      // create a new bucket
      synchronized (sync) {
        // slice
        final V output = bucket;
        bucket = aggregator.init();
        // saves output to TSOutputGenerator
        LOG.log(Level.FINE, "Save partial output : [" + prevSliceTime + "-" + nextSliceTime + "]"
            + ", output: " + output);
        tsOutputGenerator.savePartialOutput(prevSliceTime, nextSliceTime, output);
      }
      prevSliceTime = nextSliceTime;
      nextSliceTime = tsOutputGenerator.nextSliceTime();
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
      // partial aggregation
      bucket = aggregator.partialAggregate(bucket, val);
    }
  }
}
