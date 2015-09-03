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
import edu.snu.tempest.operator.window.timescale.Timescale;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * DependencyGraph shows which nodes are related to each other. This is used so that unnecessary outputs are not saved.
 */
public class DependencyGraph {

  private static final Logger LOG = Logger.getLogger(StaticComputationReuser.class.getName());

  /**
   * A table containing DependencyGraphNode for final outputs.
   */
  private final DefaultOutputLookupTableImpl<DependencyGraphNode> table;

  /**
   * A table containing DependencyGraphNode for partial outputs.
   */
  private final DefaultOutputLookupTableImpl<DependencyGraphNode> partialOutputTable;

  /**
   * The list of timescale.
   */
  private final List<Timescale> timescales;

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
   * DependencyGraph constructor. This first builds the dependency graph.
   * @param timescales the timescales that are added
   * @param startTime the initial start time of when the graph is built.
   */
  public DependencyGraph(final List<Timescale> timescales, final long startTime){
    this.sliceQueue = new LinkedList<>();
    this.table = new DefaultOutputLookupTableImpl<>();
    this.partialOutputTable = new DefaultOutputLookupTableImpl<>();
    this.timescales = timescales;
    this.period = calculatePeriod(timescales);
    this.startTime = startTime;

    // create dependency graph.
    addSlicedWindowNodeAndEdge();
    addOverlappingWindowNodeAndEdge();
  }

  /**
   * Searches for the Node that corresponds to wStartTime and wEndTime.
   */
  public int getNodeRefCount(final long wStartTime, final long wEndTime){
    long start = adjStartTime(wStartTime);
    final long end = adjEndTime(wEndTime);

    if (start >= end) {
      start -= period;
    }

    DependencyGraphNode node = null;
    try {
      node = table.lookup(start, end);
      LOG.log(Level.FINE, "Lookup! Start : " + start + " end : " + end);
    } catch (final NotFoundException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    return node.initialRefCnt;
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
        LOG.log(Level.FINE, "Saved to table : (" + start + " , " + time + ") refCount : " + parent.refCnt);
        LOG.log(Level.FINE, "(" + start + ", " + time + ") dependencies2: " + childNodes);
      }
    }
  }

  /**
   * Find period of repeated pattern.
   * period = c * lcm ( i_{1}, i_{2}, ..., i_{k} ) ( i_{k} is interval of k-th timescale)
   * c is natural number which satisfies period >= largest_window_size
   */
  private long calculatePeriod(final List<Timescale> timescalesList) {
    long newPeriod = 0;
    long largestWindowSize = 0;

    for (final Timescale ts : timescalesList) {
      if (newPeriod == 0) {
        newPeriod = ts.intervalSize;
      } else {
        newPeriod = lcm(newPeriod, ts.intervalSize);
      }
      // find largest window size
      if (largestWindowSize < ts.windowSize) {
        largestWindowSize = ts.windowSize;
      }
    }

    if (newPeriod < largestWindowSize) {
      final long div = largestWindowSize / newPeriod;
      if (largestWindowSize % newPeriod == 0) {
        newPeriod *= div;
      } else {
        newPeriod *= (div+1);
      }
    }
    return newPeriod;
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
     * A reference count to be referenced by other nodes.
     */
    private int refCnt;

    /**
     * An initial reference count.
     */
    private int initialRefCnt;

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
      this.refCnt = 0;
      this.start = start;
      this.end = end;
    }

    /**
     * Add dependent node.
     * @param n a dependent node
     */
    public void addDependency(final DependencyGraphNode n) {
      if (n == null) {
        throw new NullPointerException();
      }
      n.increaseRefCnt();
    }

    private void increaseRefCnt() {
      refCnt++;
      initialRefCnt = refCnt;
    }

    public String toString() {
      final StringBuilder sb = new StringBuilder();
      sb.append("(refCnt: ");
      sb.append(refCnt);
      sb.append(", range: [");
      sb.append(start);
      sb.append("-");
      sb.append(end);
      sb.append("], outputSaved: ");
      sb.append(")");
      return sb.toString();
    }
  }

}
