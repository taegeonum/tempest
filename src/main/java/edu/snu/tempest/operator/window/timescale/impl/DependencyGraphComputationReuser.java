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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * DependencyGraphComputationReuser.
 *
 * It saves final aggregation outputs according to the reference count of the current dependencyGraph.
 * If the node is not going to be accessed, it deletes the node.
 */
public final class DependencyGraphComputationReuser<I, T> implements ComputationReuser<T> {
  private static final Logger LOG = Logger.getLogger(DependencyGraphComputationReuser.class.getName());

  /**
   * Final aggregator.
   */
  private final CAAggregator<I, T> finalAggregator;

  /**
   * A dynamic Table for saving outputs.
   */
  private DefaultOutputLookupTableImpl<TableNode> dynamicTable;

  /**
   * An output cleaner removing stale outputs.
   */
  private final DefaultOutputCleaner cleaner;

  /**
   * The time when the operator is launched.
   */
  private final long launchTime;

  /**
   * Parallel tree aggregator.
   */
  private final ParallelTreeAggregator<I, T> parallelAggregator;

  /**
   * Dependency graph.
   */
  private AtomicReference<DependencyGraph> dependencyGraph;

  /**
   * The list of timescale.
   */
  private List<Timescale> timescales;

  /**
   * DependencyGraphComputationReuser constructor.
   * @param tsParser timescale parser
   * @param finalAggregator an aggregator for final aggregation.
   */
  @Inject
  private DependencyGraphComputationReuser(final TimescaleParser tsParser,
                                           final CAAggregator<I, T> finalAggregator,
                                           @Parameter(StartTime.class) final long launchTime,
                                           @Parameter(NumThreads.class) final int numThreads) {
    this.finalAggregator = finalAggregator;
    this.dynamicTable = new DefaultOutputLookupTableImpl<>();
    this.timescales = tsParser.timescales;
    this.launchTime = launchTime;
    this.cleaner = new DefaultOutputCleaner(timescales, dynamicTable, launchTime);
    this.parallelAggregator = new ParallelTreeAggregator<>(numThreads, numThreads * 2, finalAggregator);

    //Create new dependencyGraph.
    this.dependencyGraph = new AtomicReference<>(new DependencyGraph(timescales, launchTime));
  }

  /**
   * Save partial output.
   * @param startTime start time of the output
   * @param endTime end time of the output
   * @param output output
   */
  @Override
  public void savePartialOutput(final long startTime, final long endTime, final T output) {
    //Save all partials to dynamicTable.
    dynamicTable.saveOutput(startTime, endTime, new TableNode(output, -1, startTime, endTime));
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
    boolean isFullyProcessed = true;
    LOG.log(Level.FINE, "start : " + startTime + " end : " + endTime);

    // fetch dependent outputs
    final List<TableNode> actualChildren = new LinkedList<>();
    while (start < endTime) {
      WindowTimeAndOutput<TableNode> elem;
      try {
        elem = dynamicTable.lookupLargestSizeOutput(start, endTime);
        if ((start == startTime && elem.endTime == endTime) && elem.output.output == null) {
          // prevents self reference
          elem = dynamicTable.lookupLargestSizeOutput(start, endTime-1);
        }

        LOG.log(Level.FINE, ts + " Lookup : (" + start + ", " + endTime + ")");
        if (start == elem.endTime) {
          //if the largest output's endTime equals start, the output is not fully processed.
          isFullyProcessed = false;
          break;
        } else {
          final TableNode dependentNode = elem.output;
          LOG.log(Level.FINE, "child of " + startTime + "-" + endTime + ":" + elem.startTime + "-" + elem.endTime);
          //add the output so it can be aggregated.
          actualChildren.add(dependentNode);
          start = elem.endTime;
        }
      } catch (final NotFoundException e) {
        //if not found, search from start+1 to endTime. The node will not be fully processed.
        start += 1;
        isFullyProcessed = false;
      }
    }

    if (!isFullyProcessed) {
      LOG.log(Level.WARNING, "The output of " + ts
              + " at " + endTime + " is not fully produced. "
              + "It only happens when the timescale is recently added" + startTime);
    }

    // aggregates dependent outputs
    int i = 0;
    List<T> dependentOutputs = new LinkedList<>();
    final int size = actualChildren.size();
    while (!actualChildren.isEmpty()) {
      final Iterator<TableNode> iterator = actualChildren.iterator();
      while (iterator.hasNext()) {
        final TableNode child = iterator.next();
        if (child.output != null) {
          dependentOutputs.add(child.output);
          //since the output has been used, decrease its reference count. If it becomes 0, delete the node.
          child.decreaseRefCnt();
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
      } else {
        final List<T> dependents = dependentOutputs;
        dependentOutputs = new LinkedList<>();
        final T result = parallelAggregator.doParallelAggregation(dependents);
        dependentOutputs.add(result);
      }
    }

    final T finalResult = dependentOutputs.get(0);
    try {
      final TableNode outputNode = dynamicTable.lookup(startTime, endTime);
      LOG.log(Level.FINE, "Saves output of : " + ts +
          "[" + startTime + "-" + endTime + "]");
      //Notify waiting threads if outputNode exists
      outputNode.output = finalResult;
    } catch (final NotFoundException e) {
      // do nothing if outputNode does not exist
    }

    // remove stale outputs.
    cleaner.onNext(endTime);
    return new DepOutputAndResult<>(size, finalResult);
  }

  @Override
  public void saveOutputInformation(final long startTime, final long endTime, final Timescale ts) {
    //lookup the node in the dynamicTable. If its refCount is > 0, save the output with the same refCount.
    final int refCount = dependencyGraph.get().getNodeRefCount(endTime - ts.windowSize, endTime);
    if (refCount > 0) {
      try {
        // if exists, do not save. It can occur when partial outputs have same start and end time.
        // Ex. windowSize = 2, interval = 2
        final TableNode outputNode = dynamicTable.lookup(startTime, endTime);
        if (outputNode.refCount.get() > 0) {
          outputNode.refCount.addAndGet(refCount);
        }
      } catch (final NotFoundException e) {
        // if does not exist, save.
        final TableNode outputNode = new TableNode(null, refCount, startTime, endTime);
        dynamicTable.saveOutput(startTime, endTime, outputNode);
      }
    }
  }

  /**
   * Create new dependencyGraph when timescale is added.
   * @param ts timescale to be added.
   * @param addTime the time when timescale is added.
   */
  @Override
  public void onTimescaleAddition(final Timescale ts, final long addTime) {
    LOG.log(Level.INFO, "addTimescale " + ts);
    cleaner.onTimescaleAddition(ts, addTime);

    //Add the new timescale.
    timescales.add(ts);
    //Create new dependencyGraph.
    this.dependencyGraph = new AtomicReference<>(new DependencyGraph(timescales, launchTime));
  }

  /**
   * Create new dependencyGraph when timescale is deleted.
   * @param ts timescale to be added.
   * @param deleteTime the time when timescale is removed.
   */
  @Override
  public void onTimescaleDeletion(final Timescale ts, final long deleteTime) {
    LOG.log(Level.INFO, "removeTimescale " + ts);
    cleaner.onTimescaleDeletion(ts, deleteTime);

    //remove timescale.
    timescales.remove(ts);
    //Create new dependencyGraph
    this.dependencyGraph = new AtomicReference<>(new DependencyGraph(timescales, launchTime));
  }

  /**
   * Nodes that go in the dynamicTable.
   */
  final class TableNode {
    private T output;
    private AtomicInteger refCount;
    private final long wStartTime;
    private final long wEndTime;

    public TableNode(T output, int refCount, final long wStartTime,
                     final long wEndTime){
      this.output = output;
      this.refCount = new AtomicInteger(refCount);
      this.wStartTime = wStartTime;
      this.wEndTime = wEndTime;
    }

    /**
     * Decrease reference count of TableNode.
     * If the reference count is zero, then it removes the saved output
     * and resets the reference count to initial count
     * in order to reuse this node.
     */
    public synchronized void decreaseRefCnt() {
      if (refCount.get() > 0) {
        if (refCount.decrementAndGet() == 0) {
          dynamicTable.deleteOutput(wStartTime, wEndTime);
        }
      }
    }
  }
}