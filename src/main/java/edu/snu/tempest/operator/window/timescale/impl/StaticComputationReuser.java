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
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * StaticComputationReuserImpl Implementation.
 * It constructs dependency graph at start time.
 */
public final class StaticComputationReuser<I, T> implements ComputationReuser<T> {
  private static final Logger LOG = Logger.getLogger(StaticComputationReuser.class.getName());

  /**
   * A table containing DependencyGraphNode for final output.
   */
  private final DefaultOutputLookupTableImpl<DependencyGraphNode> table;

  /**
   * A table containing DependencyGraphNode for partial output.
   */
  private final DefaultOutputLookupTableImpl<DependencyGraphNode> partialOutputTable;

  /**
   * The list of timescale.
   */
  private final List<Timescale> timescales;

  /**
   * Aggregator for final aggregation.
   */
  private final CAAggregator<I, T> finalAggregator;

  /**
   * A period of the repeated pattern.
   */
  private final long period;

  /**
   * A start time.
   */
  private final long startTime;

  /**
   * A queue containing next slice time.
   */
  private final List<Long> sliceQueue;

  /**
   * Parallel tree aggregator.
   */
  private final ParallelTreeAggregator<I, T> parallelAggregator;

  /**
   * StaticComputationReuserImpl Implementation.
   * @param tsParser timescale parser
   * @param finalAggregator a final aggregator
   * @param startTime an initial start time
   */
  @Inject
  private StaticComputationReuser(
      final TimescaleParser tsParser,
      final CAAggregator<I, T> finalAggregator,
      @Parameter(StartTime.class) final long startTime,
      @Parameter(NumThreads.class) final int numThreads) {
    this.sliceQueue = new LinkedList<>();
    this.table = new DefaultOutputLookupTableImpl<>();
    this.partialOutputTable = new DefaultOutputLookupTableImpl<>();
    this.timescales = tsParser.timescales;
    this.finalAggregator = finalAggregator;
    this.period = calculatePeriod(timescales);
    this.startTime = startTime;
    this.parallelAggregator = new ParallelTreeAggregator<>(numThreads, numThreads * 2, finalAggregator);
    LOG.log(Level.INFO, StaticComputationReuser.class + " started. PERIOD: " + period);

    // create dependency graph.
    addSlicedWindowNodeAndEdge();
    addOverlappingWindowNodeAndEdge();
  }

  /**
   * Save a partial output into the table.
   * @param wStartTime start time of the output
   * @param wEndTime end time of the output
   * @param output output
   */
  @Override
  public void savePartialOutput(final long wStartTime,
                                final long wEndTime,
                                final T output) {
    long start = adjStartTime(wStartTime);
    final long end = adjEndTime(wEndTime);

    if (start >= end) {
      start -= period;
    }
    LOG.log(Level.FINE,
        "savePartialOutput: " + wStartTime + " - " + wEndTime +", " + start  + " - " + end);

    DependencyGraphNode node = null;
    try {
      node = partialOutputTable.lookup(start, end);
    } catch (final NotFoundException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    node.saveOutput(output);
  }

  /**
   * Aggregates partial outputs and produces a final output.
   * Dependent outputs can be aggregated into final output.
   * The dependency information is statically constructed at start time.
   * @param wStartTime start time of the output
   * @param wEndTime end time of the output
   * @param ts timescale
   * @return an aggregated output ranging from startTime to endTime.
   */
  @Override
  public T finalAggregate(final long wStartTime, final long wEndTime, final Timescale ts) {
    LOG.log(Level.FINE, "Lookup " + wStartTime + ", " + wEndTime);

    long start = adjStartTime(wStartTime);
    final long end = adjEndTime(wEndTime);
    LOG.log(Level.FINE, "final aggregate: [" + start+ "-" + end +"]");

    // reuse!
    if (start >= end) {
      start -= period;
    }

    DependencyGraphNode node = null;
    try {
      node = table.lookup(start, end);
    } catch (final NotFoundException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    final List<T> dependentOutputs = new LinkedList<>();
    for (final DependencyGraphNode child : node.getDependencies()) {
      synchronized (child) {
        if (child.getOutput() != null) {
          dependentOutputs.add(child.getOutput());
          child.decreaseRefCnt();
        } else if (wStartTime < startTime && end <= child.start) {
          // if there are no dependent outputs to use
          // no need to wait for the child's output.
          LOG.log(Level.FINE, "no wait. wStartTime: " + wStartTime + ", wEnd:" + wEndTime
              + " child.start: " + child.start + "child.end: " + child.end);
        } else {
          // if there is a dependent output that could be used
          // wait for the aggregation to finish.
          LOG.log(Level.FINE, "wait. wStartTime: " + wStartTime + ", wEnd:" + wEndTime
              + " child.start: " + child.start + "child.end: " + child.end);
          try {
            child.wait();
            final T out = child.getOutput();
            dependentOutputs.add(out);
            child.decreaseRefCnt();
          } catch (final InterruptedException e) {
            e.printStackTrace();
          }
        }
      }
    }

    // aggregates dependent outputs
    final T finalResult = parallelAggregator.doParallelAggregation(dependentOutputs);
    // save the output
    if (node.getInitialRefCnt() != 0) {
      synchronized (node) {
        node.saveOutput(finalResult);
        // after saving the output, notify the thread that is waiting for this output.
        node.notifyAll();
      }
    }
    LOG.log(Level.FINE, "finalAggregate: " + wStartTime + " - " + wEndTime + ", " + start + " - " + end
        + ", period: " + period + ", dep: " + node.getDependencies().size() + ", listLen: " + dependentOutputs.size());
    return finalResult;
  }

  @Override
  public void onTimescaleAddition(final Timescale timescale, final long addTime) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public void onTimescaleDeletion(final Timescale timescale, final long deleteTime) {
    throw new RuntimeException("Not supported");
  }

  /**
   * Adjust current time to fetch a corresponding node.
   * For example, if current time is 31 but period is 15,
   * then it adjust start time to 1.
   * @param time current time
   */
  private long adjStartTime(final long time) {
    if (time < startTime) {
      return (time - startTime) % period + period;
    } else {
      return (time - startTime) % period;
    }
  }

  /**
   * Adjust current time to fetch a corresponding node.
   * For example, if current time is 30 and period is 15,
   * then it adjust end time to 15.
   * if current time is 31 and period is 15,
   *  then it adjust end time to 1.
   * @param time current time
   */
  private long adjEndTime(final long time) {
    final long adj = (time - startTime) % period == 0 ? period : (time - startTime) % period;
    LOG.log(Level.FINE, "adjEndTime: time: " + time + ", adj: " + adj);
    return adj;
  }

  /**
   * It creates the list of next slice time.
   * This method is based on "On-the-Fly Sharing for Streamed Aggregation" paper.
   * Similar to initializeWindowState function
   */
  private void addSlicedWindowNodeAndEdge() {
    // add sliced window edges
    for (final Timescale ts : timescales) {
      final long pairedB = ts.windowSize % ts.intervalSize;
      final long pairedA = ts.intervalSize - pairedB;
      long time = pairedA;
      boolean odd = true;

      while(time <= period) {
        sliceQueue.add(time);
        if (odd) {
          time += pairedB;
        } else {
          time += pairedA;
        }
        odd = !odd;
      }
    }

    Collections.sort(sliceQueue);
    long wStartTime = 0;
    for (final long endTime : sliceQueue) {
      if (wStartTime != endTime) {
        partialOutputTable.saveOutput(wStartTime, endTime, new DependencyGraphNode(wStartTime, endTime));
        wStartTime = endTime;
      }
    }
    LOG.log(Level.FINE, "Sliced queue: " + sliceQueue);
  }

  /**
   * Find child nodes which are included in the range of [start-end].
   * For example, if the range is [0-10]
   * it finds dependency graph nodes which are included in the range of [0-10].
   * @param start start time of the output
   * @param end end time of the output.
   * @return child nodes
   */
  private List<DependencyGraphNode> findChildNodes(final long start, final long end) {
    final List<DependencyGraphNode> childNodes = new LinkedList<>();
    // find child nodes.
    long st = start;
    while (st < end) {
      WindowTimeAndOutput<DependencyGraphNode> elem = null;
      try {
        elem = table.lookupLargestSizeOutput(st, end);
        if (st == elem.endTime) {
          break;
        } else {
          childNodes.add(elem.output);
          st = elem.endTime;
        }
      } catch (final NotFoundException e) {
        try {
          elem = table.lookupLargestSizeOutput(st + period, period);
          childNodes.add(elem.output);
          st = elem.endTime - period;
        } catch (final NotFoundException e1) {
          // do nothing
        }
      }

      // find from partial output table.
      if (elem == null) {
        try {
          elem = partialOutputTable.lookupLargestSizeOutput(st, end);
          if (st == elem.endTime) {
            break;
          } else {
            childNodes.add(elem.output);
            st = elem.endTime;
          }
        } catch (final NotFoundException e) {
          try {
            elem = partialOutputTable.lookupLargestSizeOutput(st + period, period);
            childNodes.add(elem.output);
            st = elem.endTime - period;
          } catch (final NotFoundException e1) {
            e1.printStackTrace();
            throw new RuntimeException(e1);
          }
        }
      }
    }
    return childNodes;
  }

  /**
   * Add DependencyGraphNode and connect dependent nodes.
   */
  private void addOverlappingWindowNodeAndEdge() {
    for (final Timescale timescale : timescales) {
      for (long time = timescale.intervalSize; time <= period; time += timescale.intervalSize) {
        // create vertex and add it to the table cell of (time, windowsize)
        final long start = time - timescale.windowSize;
        final DependencyGraphNode parent = new DependencyGraphNode(start, time);
        final List<DependencyGraphNode> childNodes = findChildNodes(start, time);
        LOG.log(Level.FINE, "(" + start + ", " + time + ") dependencies1: " + childNodes);
        for (final DependencyGraphNode elem : childNodes) {
          parent.addDependency(elem);
        }
        table.saveOutput(start, time, parent);
        LOG.log(Level.FINE, "(" + start + ", " + time + ") dependencies2: " + childNodes);
      }
    }
  }

  /**
   * Find period of repeated pattern.
   * period = c * lcm ( i_{1}, i_{2}, ..., i_{k} ) ( i_{k} is interval of k-th timescale)
   * c is natural number which satisfies period >= largest_window_size
   */
  public static long calculatePeriod(final List<Timescale> timescales) {
    long period = 0;
    long largestWindowSize = 0;

    for (final Timescale ts : timescales) {
      if (period == 0) {
        period = ts.intervalSize;
      } else {
        period = lcm(period, ts.intervalSize);
      }
      // find largest window size
      if (largestWindowSize < ts.windowSize) {
        largestWindowSize = ts.windowSize;
      }
    }

    if (period < largestWindowSize) {
      final long div = largestWindowSize / period;
      if (largestWindowSize % period == 0) {
        period *= div;
      } else {
        period *= (div+1);
      }
    }
    return period;
  }

  private static long gcd(long a, long b) {
    while (b > 0) {
      final long temp = b;
      b = a % b; // % is remainder
      a = temp;
    }
    return a;
  }

  private static long lcm(final long a, final long b) {
    return a * (b / gcd(a, b));
  }

  /**
   * DependencyGraphNode.
   */
  final class DependencyGraphNode {
    /**
     * A list of dependent nodes.
     */
    private final List<DependencyGraphNode> dependencies;

    /**
     * A reference count to be referenced by other nodes.
     */
    private int refCnt;

    /**
     * An initial reference count.
     */
    private int initialRefCnt;

    /**
     * An output.
     */
    private T output;

    /**
     * The start time of the node.
     */
    public final long start;

    /**
     * The end time of the node.
     */
    public final long end;

    /**
     * DependencyGraphNode.
     * @param start the start time of the node.
     * @param end tbe end time of the node.
     */
    public DependencyGraphNode(final long start, final long end) {
      this.dependencies = new LinkedList<>();
      this.refCnt = 0;
      this.start = start;
      this.end = end;
    }

    /**
     * Decrease reference count of DependencyGraphNode.
     * If the reference count is zero, then it removes the saved output
     * and resets the reference count to initial count
     * in order to reuse this node.
     */
    public synchronized void decreaseRefCnt() {
      if (refCnt > 0) {
        refCnt--;
        if (refCnt == 0) {
          // Remove output
          LOG.log(Level.FINE, "Release: [" + start + "-" + end + "]");
          output = null;
          refCnt = initialRefCnt;
        }
      }
    }

    /**
     * Add dependent node.
     * @param n a dependent node
     */
    public void addDependency(final DependencyGraphNode n) {
      if (n == null) {
        throw new NullPointerException();
      }
      dependencies.add(n);
      n.increaseRefCnt();
    }

    private void increaseRefCnt() {
      refCnt++;
      initialRefCnt = refCnt;
    }

    /**
     * Get number of parent nodes.
     * @return number of parent nodes.
     */
    public int getInitialRefCnt() {
      return initialRefCnt;
    }

    /**
     * Get child (dependent) nodes.
     * @return child nodes
     */
    public List<DependencyGraphNode> getDependencies() {
      return dependencies;
    }

    public String toString() {
      final boolean outputExists = !(output == null);
      final StringBuilder sb = new StringBuilder();
      sb.append("(refCnt: ");
      sb.append(refCnt);
      sb.append(", range: [");
      sb.append(start);
      sb.append("-");
      sb.append(end);
      sb.append("], outputSaved: ");
      sb.append(outputExists);
      sb.append(")");
      return sb.toString();
    }

    public T getOutput() {
      return output;
    }

    public void saveOutput(final T value) {
      this.output = value;
    }
  }
}
