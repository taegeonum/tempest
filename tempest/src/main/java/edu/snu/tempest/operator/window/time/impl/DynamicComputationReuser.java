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
package edu.snu.tempest.operator.window.time.impl;

import edu.snu.tempest.operator.common.NotFoundException;
import edu.snu.tempest.operator.window.aggregator.CAAggregator;
import edu.snu.tempest.operator.window.time.Timescale;
import edu.snu.tempest.operator.window.time.TimescaleParser;
import edu.snu.tempest.operator.window.time.parameter.StartTime;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * DynamicComputationReuser for DynamicMTSOperatorImpl.
 *
 * It saves final aggregation outputs according to the cachingPolicy
 * and reuses the cached outputs when doing final aggregation.
 */
public final class DynamicComputationReuser<I, T> implements ComputationReuser<T> {
  private static final Logger LOG = Logger.getLogger(DynamicComputationReuser.class.getName());

  /**
   * Final aggregator.
   */
  private final CAAggregator<I, T> finalAggregator;

  /**
   * A table for saving outputs.
   */
  private final DefaultOutputLookupTableImpl<T> table;

  /**
   * An output cleaner removing stale outputs.
   */
  private final DefaultOutputCleaner cleaner;

  /**
   * A caching policy to determine output caching.
   */
  private final CachingPolicy cachingPolicy;

  /**
   * DynamicComputationReuser.
   * @param tsParser timescale parser
   * @param finalAggregator an aggregator for final aggregation.
   * @param cachingPolicy a caching policy for output caching
   * @param startTime an initial start time of the OTFMTSOperator.
   */
  @Inject
  private DynamicComputationReuser(final TimescaleParser tsParser,
                                   final CAAggregator<I, T> finalAggregator,
                                   final CachingPolicy cachingPolicy,
                                   @Parameter(StartTime.class) final long startTime) {
    this.finalAggregator = finalAggregator;
    this.table = new DefaultOutputLookupTableImpl<>();
    this.cleaner = new DefaultOutputCleaner(tsParser.timescales, table, startTime);
    this.cachingPolicy = cachingPolicy;
  }

  /**
   * Save partial output.
   * @param startTime start time of the output
   * @param endTime end time of the output
   * @param output output
   */
  @Override
  public void savePartialOutput(final long startTime, final long endTime, final T output) {
    table.saveOutput(startTime, endTime, output);
  }

  /**
   * Produces a final output by doing computation reuse..
   * It saves the final result and reuses it for other timescales' final aggregation
   * @param startTime start time of the output
   * @param endTime end time of the output
   * @param ts timescale
   * @return an aggregated output ranging from startTime to endTime.
   */
  @Override
  public T finalAggregate(final long startTime, final long endTime, final Timescale ts) {
    final long aggStartTime = System.nanoTime();
    final List<T> dependentOutputs = new LinkedList<>();
    // lookup dependencies
    long start = startTime;
    boolean isFullyProcessed = true;

    // fetch dependent outputs
    while(start < endTime) {
      final WindowTimeAndOutput<T> elem;
      try {
        elem = table.lookupLargestSizeOutput(start, endTime);
        LOG.log(Level.FINE, ts + " Lookup : (" + start + ", " + endTime + ")");
        if (start == elem.endTime) {
          isFullyProcessed = false;
          break;
        } else {
          dependentOutputs.add(elem.output);
          start = elem.endTime;
        }
      } catch (NotFoundException e) {
        start += 1;
        isFullyProcessed = false;
      }
    }

    if (!isFullyProcessed) {
      LOG.log(Level.WARNING, "The output of " + ts
          + " at " + endTime + " is not fully produced. "
          + "It only happens when the timescale is recently added");
    }

    // aggregates dependent outputs
    final T finalResult = finalAggregator.aggregate(dependentOutputs);
    LOG.log(Level.FINE, "AGG TIME OF " + ts + ": "
        + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - aggStartTime)
        + " at " + endTime + ", dependent size: " + dependentOutputs.size());

    if (cachingPolicy.cache(startTime, endTime, ts)) {
      LOG.log(Level.FINE, "Saves output of : " + ts +
          "[" + startTime + "-" + endTime + "]");
      table.saveOutput(startTime, endTime, finalResult);
    }

    // remove stale outputs.
    cleaner.onNext(endTime);
    return finalResult;
  }

  /**
   * Adjust output cleaner and caching policy.
   * @param ts timescale to be added.
   * @param addTime the time when timescale is added.
   */
  @Override
  public void onTimescaleAddition(final Timescale ts, final long addTime) {
    LOG.log(Level.FINE, "addTimescale " + ts);
    cleaner.onTimescaleAddition(ts, addTime);
    cachingPolicy.onTimescaleAddition(ts, addTime);
  }

  /**
   * Adjust output cleaner and caching policy.
   * @param ts timescale to be added.
   * @param deleteTime the time when timescale is removed.
   */
  @Override
  public void onTimescaleDeletion(final Timescale ts, final long deleteTime) {
    LOG.log(Level.INFO, "removeTimescale " + ts);
    cleaner.onTimescaleDeletion(ts, deleteTime);
    cachingPolicy.onTimescaleDeletion(ts, deleteTime);
  }
}
