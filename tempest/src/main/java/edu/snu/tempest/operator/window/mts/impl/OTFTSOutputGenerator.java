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
package edu.snu.tempest.operator.window.mts.impl;

import edu.snu.tempest.operator.common.NotFoundException;
import edu.snu.tempest.operator.window.FinalAggregator;
import edu.snu.tempest.operator.window.Timescale;
import edu.snu.tempest.operator.window.common.DefaultOutputLookupTableImpl;
import edu.snu.tempest.operator.window.common.DynamicOutputCleaner;
import edu.snu.tempest.operator.window.common.TSOutputGenerator;
import edu.snu.tempest.operator.window.common.WindowingTimeAndOutput;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * OTFComputationReuser for OTFMTSOperatorImpl.
 * Compared to computation reuse (save final aggregation) in DynamicComputationReuser and StaticComputationReuser,
 * it just saves partial aggregation and does not do computation reuse.
 */
final class OTFTSOutputGenerator<T> implements TSOutputGenerator<T> {
  private static final Logger LOG = Logger.getLogger(OTFTSOutputGenerator.class.getName());

  /**
   * An aggregator for final aggregation.
   */
  private final FinalAggregator<T> finalAggregator;

  /**
   * A table for saving partial/final outputs.
   */
  private final DefaultOutputLookupTableImpl<T> table;

  /**
   * An output cleaner for removing stale outputs in the table.
   */
  private final DynamicOutputCleaner outputCleaner;

  /**
   * OTFTSOutputGenerator.
   * Compared to computation reuse (save final aggregation) in tempest MTS operators,
   * it just saves partial aggregation.
   * @param timescales an intial timescales.
   * @param finalAggregator an aggregator for final aggregation.
   * @param startTime an initial start time of the OTFMTSOperator.
   */
  @Inject
  public OTFTSOutputGenerator(final List<Timescale> timescales,
                              final FinalAggregator<T> finalAggregator,
                              final long startTime) {
    this.finalAggregator = finalAggregator;
    this.table = new DefaultOutputLookupTableImpl<>();
    this.outputCleaner = new DynamicOutputCleaner(timescales, table, startTime);
  }

  /**
   * Save a partial output containing data starting from the startTime to endTime.
   * @param startTime start time of the output
   * @param endTime end time of the output
   * @param output output
   */
  @Override
  public void savePartialOutput(final long startTime, final long endTime, final T output) {
    table.saveOutput(startTime, endTime, output);
  }

  /**
   * It just produces the final result
   * and does not save the output.
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
      final WindowingTimeAndOutput<T> elem;
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
    final T finalResult = finalAggregator.finalAggregate(dependentOutputs);
    LOG.log(Level.FINE, "AGG TIME OF " + ts + ": "
        + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - aggStartTime)
        + " at " + endTime + ", dependent size: " + dependentOutputs.size());

    // remove
    outputCleaner.onNext(endTime);
    return finalResult;
  }

  /**
   * Adjust garbage collector.
   * @param ts timescale to be added.
   * @param startTime the time when timescale is added..
   */
  public void addTimescale(final Timescale ts, final long startTime) {
    LOG.log(Level.INFO, "addTimescale " + ts);
    outputCleaner.addTimescale(ts, startTime);
  }

  /**
   * Adjust garbage collector.
   * @param ts timescale to be deleted.
   */
  public void removeTimescale(final Timescale ts) {
    LOG.log(Level.INFO, "removeTimescale " + ts);
    outputCleaner.removeTimescale(ts);
  }
}
