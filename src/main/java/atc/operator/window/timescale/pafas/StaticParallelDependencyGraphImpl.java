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

package atc.operator.window.timescale.pafas;

import org.apache.reef.tang.annotations.Parameter;
import atc.operator.common.NotFoundException;
import atc.operator.window.timescale.Timescale;
import atc.operator.window.timescale.common.OutputLookupTable;
import atc.operator.window.timescale.common.SharedForkJoinPool;
import atc.operator.window.timescale.common.TimescaleParser;
import atc.operator.window.timescale.common.Timespan;
import atc.operator.window.timescale.parameter.StartTime;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ForkJoinTask;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * DependencyGraph shows which nodes are related to each other. This is used so that unnecessary outputs are not saved.
 */
public final class StaticParallelDependencyGraphImpl<T> implements DependencyGraph {

  private static final Logger LOG = Logger.getLogger(StaticParallelDependencyGraphImpl.class.getName());

  /**
   * A table containing DependencyGraphNode for partial outputs.
   */
  //private final DefaultOutputLookupTableImpl<DependencyGraphNode> partialOutputTable;

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

  private final PartialTimespans<T> partialTimespans;
  private final OutputLookupTable<Node<T>> finalTimespans;
  private final SelectionAlgorithm<T> selectionAlgorithm;
  private final SharedForkJoinPool sharedForkJoinPool;

  /**
   * DependencyGraph constructor. This first builds the dependency graph.
   * @param startTime the initial start time of when the graph is built.
   */
  @Inject
  private StaticParallelDependencyGraphImpl(final TimescaleParser tsParser,
                                            @Parameter(StartTime.class) final long startTime,
                                            final PartialTimespans partialTimespans,
                                            final PeriodCalculator periodCalculator,
                                            final SharedForkJoinPool sharedForkJoinPool,
                                            final OutputLookupTable<Node<T>> outputLookupTable,
                                            final SelectionAlgorithm<T> selectionAlgorithm) {
    this.partialTimespans = partialTimespans;
    this.timescales = tsParser.timescales;
    this.period = periodCalculator.getPeriod();
    this.startTime = startTime;
    this.finalTimespans = outputLookupTable;
    this.selectionAlgorithm = selectionAlgorithm;
    this.sharedForkJoinPool = sharedForkJoinPool;
    // create dependency graph.
    addOverlappingWindowNodeAndEdge();
  }

  /**
   * Adjust current time to fetch a corresponding node.
   * For example, if current time is 31 but period is 15,
   * then it adjust start time to 1.
   * @param time current time
   */
  private long adjStartTime(final long time) {
    if (time < startTime) {
      //return (time - startTime) % period + period;
      return time + period;
    } else {
      //return (time - startTime) % period;
      return startTime + (time - startTime) % period;
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
    final long adj = (time - startTime) % period == 0 ? (startTime + period) : startTime + (time - startTime) % period;
    LOG.log(Level.FINE, "adjEndTime: time: " + time + ", adj: " + adj);
    return adj;
  }

  /**
   * Add DependencyGraphNode and connect dependent nodes.
   */
  private void addOverlappingWindowNodeAndEdge() {

    // Add nodes
    final List<ForkJoinTask> tasks = new LinkedList<>();
    for (final Timescale timescale : timescales) {
      tasks.add(sharedForkJoinPool.getForkJoinPool().submit(new Runnable() {
        @Override
        public void run() {
          for (long time = timescale.intervalSize + startTime; time <= period + startTime; time += timescale.intervalSize) {
            // create vertex and add it to the table cell of (time, windowsize)
            final long start = time - timescale.windowSize;
            final Node parent = new Node(start, time, false);
            finalTimespans.saveOutput(start, time, parent);
            LOG.log(Level.FINE, "Saved to table : (" + start + " , " + time + ") refCount : " + parent.refCnt);
          }
        }
      }));
    }

    for (final ForkJoinTask task : tasks) {
      task.join();
    }

    // Add edges
    final List<ForkJoinTask> taskEdges = new LinkedList<>();
    for (final Timescale timescale : timescales) {
      taskEdges.add(sharedForkJoinPool.getForkJoinPool().submit(new Runnable() {
        @Override
        public void run() {
          for (long time = timescale.intervalSize + startTime; time <= period + startTime; time += timescale.intervalSize) {
            final long start = time - timescale.windowSize;
            final Node parent;
            try {
              parent = finalTimespans.lookup(start, time);
              final List<Node<T>> childNodes = selectionAlgorithm.selection(start, time);
              LOG.log(Level.FINE, "(" + start + ", " + time + ") dependencies1: " + childNodes);
              for (final Node<T> elem : childNodes) {
                parent.addDependency(elem);
              }
            } catch (NotFoundException e) {
              e.printStackTrace();
              throw new RuntimeException(e);
            }
          }
        }
      }));
    }

    for (final ForkJoinTask task : taskEdges) {
      task.join();
    }

    //System.out.println(partialTimespans);
  }

  @Override
  public List<Timespan> getFinalTimespans(final long t) {
    final List<Timespan> timespans = new LinkedList<>();
    for (final Timescale timescale : timescales) {
      if ((t - startTime) % timescale.intervalSize == 0) {
        timespans.add(new Timespan(t - timescale.windowSize, t, timescale));
      }
    }
    return timespans;
  }

  @Override
  public Node<T> getNode(final Timespan timespan) {
    long adjStartTime = adjStartTime(timespan.startTime);
    final long adjEndTime = adjEndTime(timespan.endTime);
    if (adjStartTime >= adjEndTime) {
      adjStartTime -= period;
    }
    //System.out.println("ORIGIN: " + timespan + ", ADJ: [" + adjStartTime + ", " + adjEndTime + ")");

    try {
      return finalTimespans.lookup(adjStartTime, adjEndTime);
    } catch (final NotFoundException e) {
      // find partial timespan
      final Node<T> partialTimespanNode = partialTimespans.getNextPartialTimespanNode(adjStartTime);
      if (partialTimespanNode.end != adjEndTime) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
      return partialTimespanNode;
    }
  }

  @Override
  public long getPeriod() {
    return period;
  }

  @Override
  public void addSlidingWindow(final Timescale ts, final long addTime) {

  }

  @Override
  public void removeSlidingWindow(final Timescale ts, final long deleteTime) {

  }
}
