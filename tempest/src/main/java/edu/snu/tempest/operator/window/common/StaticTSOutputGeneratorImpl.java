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

import edu.snu.tempest.operator.common.NotFoundException;
import edu.snu.tempest.operator.window.Aggregator;
import edu.snu.tempest.operator.window.Timescale;

import javax.inject.Inject;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * StaticTSOutputGeneratorImpl Implementation.
 *
 * It constructs RelationGraph at start time.
 */
public final class StaticTSOutputGeneratorImpl<T> implements StaticTSOutputGenerator<T> {
  private static final Logger LOG = Logger.getLogger(StaticTSOutputGeneratorImpl.class.getName());

  /**
   * A table containing RelationGraphNode for final output.
   */
  private final DefaultOutputLookupTableImpl<RelationGraphNode> table;

  /**
   * A table containing RelationGraphNode for partial output.
   */
  private final DefaultOutputLookupTableImpl<RelationGraphNode> partialOutputTable;

  /**
   * The list of timescale.
   */
  private final List<Timescale> timescales;

  /**
   * Aggregator for final aggregation.
   */
  private final Aggregator<?, T> finalAggregator;

  /**
   * A queue containing next slice time.
   */
  private final List<Long> sliceQueue;

  /**
   * A period of the repeated pattern.
   */
  private final long period;

  /**
   * An index for looking up sliceQueue.
   */
  private int index = 0;

  /**
   * An initial start time.
   */
  private final long initialStartTime;

  /**
   * A previous slice time.
   */
  private long prevSliceTime = 0;

  @Inject
  public StaticTSOutputGeneratorImpl(final List<Timescale> timescales,
                                     final Aggregator<?, T> finalAggregator,
                                     final long startTime) {
    this.table = new DefaultOutputLookupTableImpl<>();
    this.partialOutputTable = new DefaultOutputLookupTableImpl<>();
    this.timescales = timescales;
    this.finalAggregator = finalAggregator;
    this.sliceQueue = new LinkedList<>();
    this.period = calculatePeriod(timescales);
    this.initialStartTime = startTime;
    LOG.log(Level.INFO, StaticTSOutputGeneratorImpl.class + " started. PERIOD: " + period);

    // create RelationGraph.
    addSlicedWindowNodeAndEdge();
    addOverlappingWindowNodeAndEdge();
  }

  /**
   * Save a partial output into the table.
   * @param startTime start time of the output
   * @param endTime end time of the output
   * @param output output
   */
  @Override
  public void savePartialOutput(final long startTime,
                                final long endTime,
                                final T output) {
    long start = adjStartTime(startTime);
    final long end = adjEndTime(endTime);

    if (start >= end) {
      start -= period;
    }
    LOG.log(Level.FINE,
        "savePartialOutput: " + startTime + " - " + endTime +", " + start  + " - " + end);

    RelationGraphNode node = null;
    try {
      node = partialOutputTable.lookup(start, end);
    } catch (NotFoundException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    node.saveOutput(output);
  }

  /**
   * Gets next slice time for slicing input and creating partial outputs.
   *
   * SlicedWindowOperator can use this to slice input.
   */
  public synchronized long nextSliceTime() {
    long sliceTime = adjustNextSliceTime();
    while (prevSliceTime == sliceTime) {
      sliceTime = adjustNextSliceTime();
    }
    prevSliceTime = sliceTime;
    return sliceTime;
  }

  /**
   * Adjust next slice time.
   */
  private long adjustNextSliceTime() {
    return initialStartTime + (index / sliceQueue.size()) * period
        + sliceQueue.get((index++) % sliceQueue.size());
  }

  /**
   * Aggregates partial outputs and produces a final output.
   *
   * Dependent outputs can be aggregated into final output.
   * The dependency information is statically constructed at start time.
   *
   * @param startTime start time of the output
   * @param endTime end time of the output
   * @param ts timescale
   * @return an aggregated output ranging from startTime to endTime.
   */
  @Override
  public T finalAggregate(final long startTime, final long endTime, final Timescale ts) {
    LOG.log(Level.FINE, "Lookup " + startTime + ", " + endTime);

    long start = adjStartTime(startTime);
    final long end = adjEndTime(endTime);
    LOG.log(Level.FINE, "final aggregate: [" + start+ "-" + end +"]");

    // reuse!
    if (start >= end) {
      start -= period;
    }

    RelationGraphNode node = null;
    try {
      node = table.lookup(start, end);
    } catch (final NotFoundException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    final List<T> list = new LinkedList<>();
    for (final RelationGraphNode child : node.getDependencies()) {
      if (child.getOutput() != null) {
        list.add(child.getOutput());
        child.decreaseRefCnt();
      }
    }

    final T output = finalAggregator.finalAggregate(list);
    // save the output
    if (node.getRefCnt() != 0) {
      node.saveOutput(output);
    }
    LOG.log(Level.FINE, "finalAggregate: " + startTime + " - " + endTime + ", " + start + " - " + end
        + ", period: " + period + ", dep: " + node.getDependencies().size() + ", listLen: " + list.size());
    return output;
  }

  /**
   * Adjust current time to fetch a corresponding node.
   * For example, if current time is 31 but period is 15,
   * then it adjust start time to 1.
   *
   * @param time current time
   */
  private long adjStartTime(final long time) {
    if (time < initialStartTime) {
      return (time - initialStartTime) % period + period;
    } else {
      return (time - initialStartTime) % period;
    }
  }

  /**
   * Adjust current time to fetch a corresponding node.
   * For example, if current time is 30 and period is 15,
   * then it adjust end time to 15.
   *
   * if current time is 31 and period is 15,
   *  then it adjust end time to 1.
   *
   * @param time current time
   */
  private long adjEndTime(final long time) {
    final long adj = (time - initialStartTime) % period == 0 ? period : (time - initialStartTime) % period;
    LOG.log(Level.FINE, "adjEndTime: time: " + time + ", adj: " + adj);
    return adj;
  }

  /**
   * It creates the list of next slice time.
   *
   * This method is based on "On-the-Fly Sharing " paper.
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
    long startTime = 0;
    for (final long endTime : sliceQueue) {
      if (startTime != endTime) {
        partialOutputTable.saveOutput(startTime, endTime, new RelationGraphNode(startTime, endTime));
        startTime = endTime;
      }
    }
    LOG.log(Level.FINE, "Sliced queue: " + sliceQueue);
  }

  /**
   * Find child nodes which are included in the range of [start-end].
   * For example, if the range is [0-10]
   * it finds relation graph nodes which are included in the range of [0-10].
   * @param start start time of the output
   * @param end end time of the output.
   * @return child nodes
   */
  private List<RelationGraphNode> findChildNodes(final long start, final long end) {
    final List<RelationGraphNode> childNodes = new LinkedList<>();
    // find child nodes.
    long st = start;
    while (st < end) {
      WindowingTimeAndOutput<RelationGraphNode> elem = null;
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
   * Add RelationGraphNode and connect dependent nodes.
   */
  private void addOverlappingWindowNodeAndEdge() {
    for (final Timescale timescale : timescales) {
      for (long time = timescale.intervalSize; time <= period; time += timescale.intervalSize) {
        // create vertex and add it to the table cell of (time, windowsize)
        final long start = time - timescale.windowSize;
        final RelationGraphNode parent = new RelationGraphNode(start, time);
        final List<RelationGraphNode> childNodes = findChildNodes(start, time);
        LOG.log(Level.FINE, "(" + start + ", " + time + ") dependencies1: " + childNodes);
        for (final RelationGraphNode elem : childNodes) {
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
   * RelationGraphNode.
   */
  final class RelationGraphNode {
    /**
     * A list of dependent nodes.
     */
    private final List<RelationGraphNode> dependencies;

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
     * RelationGraphNode.
     * @param start the start time of the node.
     * @param end tbe end time of the node.
     */
    public RelationGraphNode(final long start, final long end) {
      this.dependencies = new LinkedList<>();
      this.refCnt = 0;
      this.start = start;
      this.end = end;
    }

    /**
     * Decrease reference count of RelationGraphNode.
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

    public void addDependency(final RelationGraphNode n) {
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

    public int getRefCnt() {
      return refCnt;
    }

    public List<RelationGraphNode> getDependencies() {
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
