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

import edu.snu.tempest.operator.common.NotFoundException;
import edu.snu.tempest.operator.window.aggregator.CAAggregator;
import edu.snu.tempest.operator.window.timescale.Timescale;
import edu.snu.tempest.operator.window.timescale.parameter.NumThreads;
import edu.snu.tempest.operator.window.timescale.parameter.StartTime;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
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
  private final DefaultOutputLookupTableImpl<DependencyGraphNode> table;

  /**
   * An output cleaner removing stale outputs.
   */
  private final DefaultOutputCleaner cleaner;

  /**
   * A caching policy to determine output caching.
   */
  private final CachingPolicy cachingPolicy;

  /**
   * Parallel tree aggregator.
   */
  private final ParallelTreeAggregator<I, T> parallelAggregator;

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
                                   @Parameter(StartTime.class) final long startTime,
                                   @Parameter(NumThreads.class) final int numThreads) {
    this.finalAggregator = finalAggregator;
    this.table = new DefaultOutputLookupTableImpl<>();
    this.cleaner = new DefaultOutputCleaner(tsParser.timescales, table, startTime);
    this.cachingPolicy = cachingPolicy;
    this.parallelAggregator = new ParallelTreeAggregator<>(numThreads, numThreads * 2, finalAggregator);
  }

  /**
   * Save partial output.
   * @param startTime start time of the output
   * @param endTime end time of the output
   * @param output output
   */
  @Override
  public void savePartialOutput(final long startTime, final long endTime, final T output) {
    //LOG.log(Level.INFO, "SAVE PARTIAL [" + startTime + "-" + endTime +"]");
    table.saveOutput(startTime, endTime, new DependencyGraphNode(output));
  }

  /**
   * Produces a final output by doing computation reuse.
   * It saves the final result and reuses it for other timescales' final aggregation
   * @param startTime start time of the output
   * @param endTime end time of the output
   * @param ts timescale
   * @return an aggregated output ranging from startTime to endTime.
   */
  @Override
  public DepOutputAndResult<T> finalAggregate(final long startTime, final long endTime, final Timescale ts) {
    final long aggStartTime = System.nanoTime();
    // lookup dependencies
    long start = startTime;
    boolean isFullyProcessed = endTime - startTime == ts.windowSize;

    // todo remove
    //final List<WindowTimeAndOutput<DependencyGraphNode>> nodes = new LinkedList<>();
    // fetch dependent outputs
    final List<DependencyGraphNode> actualChildren = new LinkedList<>();
    while (start < endTime) {
      LOG.log(Level.FINE, ts + " Lookup : (" + start + ", " + endTime + ")");
      WindowTimeAndOutput<DependencyGraphNode> elem;
      try {
        elem = table.lookupLargestSizeOutput(start, endTime);
        if ((start == startTime && elem.endTime == endTime) && elem.output.output.get() == null) {
          // prevents self reference
          elem = table.lookupLargestSizeOutput(start, endTime-1);
        }

        if (start == elem.endTime) {
          isFullyProcessed = false;
          break;
        } else {
          final DependencyGraphNode dependentNode = elem.output;
          actualChildren.add(dependentNode);
          //nodes.add(elem);
          start = elem.endTime;
        }
      } catch (final NotFoundException e) {
        start += 1;
        isFullyProcessed = false;
      }
    }

    // todo remove
    //LOG.log(Level.INFO, "LOOKUP: [" + startTime + "-" + endTime + "]" + ", ts: " + ts + ", dependents: " + nodes);

    if (!isFullyProcessed) {
      LOG.log(Level.FINE, "The output of " + ts
          + " at " + endTime + " is not fully produced. "
          + "It only happens when the timescale is recently added");
    }

    // aggregates dependent outputs
    // aggregates dependent outputs
    int i = 0;
    List<T> dependentOutputs = new LinkedList<>();
    final int size = actualChildren.size();
    while (!actualChildren.isEmpty()) {
      final Iterator<DependencyGraphNode> iterator = actualChildren.iterator();
      while (iterator.hasNext()) {
        final DependencyGraphNode child = iterator.next();
        if (child.output.get() != null) {
          dependentOutputs.add(child.output.get());
          iterator.remove();
        }
      }

      if (dependentOutputs.size() == 1 && !actualChildren.isEmpty()) {
        try {
          Thread.sleep(10 * i);
          i++;
        } catch (final InterruptedException e) {
          e.printStackTrace();
        }
      } else if (dependentOutputs.size() > 1) {
        final List<T> dependents = dependentOutputs;
        dependentOutputs = new LinkedList<>();
        final T result = parallelAggregator.doParallelAggregation(dependents);
        dependentOutputs.add(result);
      }
    }

    final T finalResult = dependentOutputs.get(0);
    try {
      final DependencyGraphNode outputNode = table.lookup(startTime, endTime);
      LOG.log(Level.FINE, "Saves output of : " + ts +
          "[" + startTime + "-" + endTime + "]");
      //Notify waiting threads if outputNode exists
      if (outputNode.output.get() == null) {
        outputNode.output.compareAndSet(null, finalResult);
      }
    } catch (final NotFoundException e) {
      // do nothing
    }

    // remove stale outputs.
    cleaner.onNext(endTime-60);
    return new DepOutputAndResult<>(size, finalResult);
  }

  @Override
  public void saveOutputInformation(final long startTime, final long endTime, final Timescale ts) {
    final boolean cache = cachingPolicy.cache(startTime, endTime, ts);
    // add a node into table before doing final aggregation.
    // If other threads look up this node, they should wait until the final aggregation is finished.
    if (cache) {
      try {
        // if exists, do not save. It can occur when partial outputs have same start and end time.
        // Ex. windowSize = 2, interval = 2
        table.lookup(startTime, endTime);
      } catch (final NotFoundException e) {
        // if does not exist, save.
        //LOG.log(Level.INFO, "CACHE [" + startTime + "-" + endTime +"], ts: " + ts);
        final DependencyGraphNode outputNode = new DependencyGraphNode();
        table.saveOutput(startTime, endTime, outputNode);
      }
    }
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

  @Override
  public void close() throws Exception {
    parallelAggregator.close();
  }

  /**
   * DependencyGraphNode which contains output.
   */
  final class DependencyGraphNode  {
    AtomicReference<T> output;

    public DependencyGraphNode() {
      this.output = new AtomicReference<>(null);
    }

    public DependencyGraphNode(final T output) {
      this.output = new AtomicReference<>(output);
    }
  }
}
