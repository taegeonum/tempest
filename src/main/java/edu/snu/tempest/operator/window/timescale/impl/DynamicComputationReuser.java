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
import edu.snu.tempest.operator.window.timescale.TimescaleParser;
import edu.snu.tempest.operator.window.timescale.parameter.StartTime;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.*;
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
   * SliceQueue containing next slice time.
   */
  private final PriorityQueue<SliceInfo> sliceQueue;

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
                                   @Parameter(StartTime.class) final long startTime) {
    this.sliceQueue = new PriorityQueue<>(10, new SliceInfoComparator());
    this.finalAggregator = finalAggregator;
    this.table = new DefaultOutputLookupTableImpl<>();
    this.cleaner = new DefaultOutputCleaner(tsParser.timescales, table, startTime);
    this.cachingPolicy = cachingPolicy;
    // TODO: #46 Parameterize the number of threads.
    this.parallelAggregator = new ParallelTreeAggregator<>(8, finalAggregator);
    initializeWindowState(startTime, tsParser.timescales);
  }

  /**
   * Initialize next slice time.
   * This method is based on "On-the-Fly Sharing for Streamed Aggregation" paper.
   * Similar to initializeWindowState function
   */
  private void initializeWindowState(final long startTime, final List<Timescale> timescales) {
    LOG.log(Level.INFO, "SlicedWindow initialization");
    for (final Timescale ts : timescales) {
      addSlices(startTime, ts);
    }
  }

  /**
   * It returns a next slice time for producing partial results.
   * This can be used for slice time to create partial results of incremental aggregation.
   * Similar to advanceWindowGetNextEdge function in the "On-the-Fly Sharing for Streamed Aggregation" paper.
   */
  @Override
  public long nextSliceTime() {
    SliceInfo info = null;
    synchronized (sliceQueue) {
      if (sliceQueue.size() == 0) {
        return 0;
      }

      final long time = sliceQueue.peek().sliceTime;
      while (time == sliceQueue.peek().sliceTime) {
        info = sliceQueue.poll();
        if (info.last) {
          addSlices(info.sliceTime, info.timescale);
        }
      }
    }
    return info.sliceTime;
  }

  /**
   * Save partial output.
   * @param startTime start time of the output
   * @param endTime end time of the output
   * @param output output
   */
  @Override
  public void savePartialOutput(final long startTime, final long endTime, final T output) {
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
  public T finalAggregate(final long startTime, final long endTime, final Timescale ts) {
    final long aggStartTime = System.nanoTime();
    final boolean cache = cachingPolicy.cache(startTime, endTime, ts);
    final List<T> dependentOutputs = new LinkedList<>();
    // lookup dependencies
    long start = startTime;
    boolean isFullyProcessed = true;

    // fetch dependent outputs
    while(start < endTime) {
      final WindowTimeAndOutput<DependencyGraphNode> elem;
      try {
        elem = table.lookupLargestSizeOutput(start, endTime);
        LOG.log(Level.FINE, ts + " Lookup : (" + start + ", " + endTime + ")");
        if (start == elem.endTime) {
          isFullyProcessed = false;
          break;
        } else {
          final DependencyGraphNode dependentNode = elem.output;
          synchronized (dependentNode) {
            // if there is a dependent output that could be used
            // wait for the aggregation to finish.
            LOG.log(Level.FINE, "Wait:  (" + start + ", " + endTime + ")");
            if (dependentNode.output == null) {
              dependentNode.wait();
            }
            LOG.log(Level.FINE, "Awake:  (" + start + ", " + endTime + ")");
          }
          dependentOutputs.add(dependentNode.output);
          start = elem.endTime;
        }
      } catch (final NotFoundException e) {
        start += 1;
        isFullyProcessed = false;
      } catch (final InterruptedException e) {
        e.printStackTrace();
      }
    }

    if (!isFullyProcessed) {
      LOG.log(Level.WARNING, "The output of " + ts
          + " at " + endTime + " is not fully produced. "
          + "It only happens when the timescale is recently added");
    }

    // add a node into table before doing final aggregation.
    // If other threads look up this node, they should wait until the final aggregation is finished.
    final DependencyGraphNode outputNode = new DependencyGraphNode();
    if (cache) {
      table.saveOutput(startTime, endTime, outputNode);
    }

    // aggregates dependent outputs
    final T finalResult = parallelAggregator.doParallelAggregation(dependentOutputs);

    LOG.log(Level.FINE, "AGG TIME OF " + ts + ": "
        + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - aggStartTime)
        + " at " + endTime + ", dependent size: " + dependentOutputs.size());

    if (cache) {
      LOG.log(Level.FINE, "Saves output of : " + ts +
          "[" + startTime + "-" + endTime + "]");
      synchronized (outputNode) {
        outputNode.output = finalResult;
        outputNode.notifyAll();
      }
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
    // Add slices
    synchronized (sliceQueue) {
      final long nst = sliceQueue.peek().sliceTime;
      addSlices(addTime, ts);
      long sliceTime = sliceQueue.peek().sliceTime;
      while (sliceTime < nst) {
        sliceTime = nextSliceTime();
      }
    }
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
    synchronized (sliceQueue) {
      for (final Iterator<SliceInfo> iterator = sliceQueue.iterator(); iterator.hasNext();) {
        final SliceInfo slice = iterator.next();
        if (slice.timescale.equals(ts)) {
          iterator.remove();
        }
      }
    }
  }

  /**
   * Add next slice time into sliceQueue.
   * Similar to addEdges function in the "On-the-Fly Sharing for Streamed Aggregation" paper.
   */
  private void addSlices(final long startTime, final Timescale ts) {
    final long pairedB = ts.windowSize % ts.intervalSize;
    final long pairedA = ts.intervalSize - pairedB;
    synchronized (sliceQueue) {
      sliceQueue.add(new SliceInfo(startTime + pairedA, ts, false));
      sliceQueue.add(new SliceInfo(startTime + pairedA + pairedB, ts, true));
    }
  }

  private final class SliceInfo {
    public final long sliceTime;
    public final Timescale timescale;
    public final boolean last;

    SliceInfo(final long sliceTime,
              final Timescale timescale,
              final boolean last) {
      this.sliceTime = sliceTime;
      this.timescale = timescale;
      this.last = last;
    }
  }

  private final class SliceInfoComparator implements Comparator<SliceInfo> {
    @Override
    public int compare(final SliceInfo o1, final SliceInfo o2) {
      if (o1.sliceTime < o2.sliceTime) {
        return -1;
      } else if (o1.sliceTime > o2.sliceTime) {
        return 1;
      } else {
        return 0;
      }
    }
  }

  /**
   * DependencyGraphNode which contains output.
   */
  final class DependencyGraphNode  {
    T output;

    public DependencyGraphNode() {
    }

    public DependencyGraphNode(final T output) {
      this.output = output;
    }
  }
}

