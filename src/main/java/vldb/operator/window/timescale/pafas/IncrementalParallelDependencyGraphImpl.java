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

package vldb.operator.window.timescale.pafas;

import org.apache.reef.tang.annotations.Parameter;
import vldb.operator.common.NotFoundException;
import vldb.operator.window.timescale.Timescale;
import vldb.operator.window.timescale.common.OutputLookupTable;
import vldb.operator.window.timescale.common.SharedForkJoinPool;
import vldb.operator.window.timescale.common.TimescaleParser;
import vldb.operator.window.timescale.common.Timespan;
import vldb.operator.window.timescale.parameter.StartTime;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ForkJoinTask;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * DependencyGraph shows which nodes are related to each other. This is used so that unnecessary outputs are not saved.
 */
public final class IncrementalParallelDependencyGraphImpl<T> implements DependencyGraph {

  private static final Logger LOG = Logger.getLogger(IncrementalParallelDependencyGraphImpl.class.getName());

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

  private long currBuildingIndex;
  private final long incrementalStep = 4500;
  private final long largestWindowSize;
  private final SharedForkJoinPool sharedForkJoinPool;

  /**
   * DependencyGraph constructor. This first builds the dependency graph.
   * @param startTime the initial start time of when the graph is built.
   */
  @Inject
  private IncrementalParallelDependencyGraphImpl(final TimescaleParser tsParser,
                                                 @Parameter(StartTime.class) final long startTime,
                                                 final PartialTimespans partialTimespans,
                                                 final PeriodCalculator periodCalculator,
                                                 final OutputLookupTable<Node<T>> outputLookupTable,
                                                 final SharedForkJoinPool sharedForkJoinPool,
                                                 final SelectionAlgorithm<T> selectionAlgorithm) {
    this.sharedForkJoinPool = sharedForkJoinPool;
    this.partialTimespans = partialTimespans;
    this.timescales = tsParser.timescales;
    this.largestWindowSize = timescales.get(timescales.size()-1).windowSize;
    this.period = periodCalculator.getPeriod();
    this.startTime = startTime;
    this.currBuildingIndex = startTime + incrementalStep;
    this.finalTimespans = outputLookupTable;
    this.selectionAlgorithm = selectionAlgorithm;
    if (period <= incrementalStep + largestWindowSize) {
      addOverlappingWindowNodeAndEdge(startTime, period + startTime);
    } else {
      // incremenatal creation of dependency graph until incremental step size.
      addFirstStepNodeAndEdge();
    }
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

  private void addFirstStepNodeAndEdge() {
    // [---------------------period------------------]
    // [<-step-><----largestWindow---->....<-behind->]

    // Add nodes until incremental step + largestWindow
    final List<ForkJoinTask> tasks = new LinkedList<>();
    for (final Timescale timescale : timescales) {
      tasks.add(sharedForkJoinPool.getForkJoinPool().submit(new Runnable() {
        @Override
        public void run() {
          for (long time = timescale.intervalSize + startTime; time <= startTime + incrementalStep + largestWindowSize; time += timescale.intervalSize) {
            // create vertex and add it to the table cell of (time, windowsize)
            final long start = time - timescale.windowSize;
            final Node parent = new Node(start, time, false);
            finalTimespans.saveOutput(start, time, parent);
            LOG.log(Level.FINE, "Saved to table : (" + start + " , " + time + ") refCount : " + parent.refCnt);
          }
        }
      }));
    }

    // Add behind nodes
    final long behindStart = Math.max(startTime + incrementalStep + largestWindowSize,
        (startTime + period - largestWindowSize + incrementalStep));
    for (final Timescale timescale : timescales) {
      tasks.add(sharedForkJoinPool.getForkJoinPool().submit(new Runnable() {
        @Override
        public void run() {
          final long align = behindStart + (timescale.intervalSize - ((behindStart - startTime) % timescale.intervalSize));
          for (long time = align; time <= period; time += timescale.intervalSize) {
            // create vertex and add it to the table cell of (time, windowsize)
            final long start = time - timescale.windowSize;
            final Node parent = new Node(start, time, false);
            finalTimespans.saveOutput(start, time, parent);
            LOG.log(Level.FINE, "Saved to table : (" + start + " , " + time + ") refCount : " + parent.refCnt);
          }
        }
      }));
    }

    // Join the tasks creating nodes
    for (final ForkJoinTask task : tasks) {
      task.join();
    }

    // Add edges for the front node
    final List<ForkJoinTask> taskEdges = new LinkedList<>();
    for (final Timescale timescale : timescales) {
      taskEdges.add(sharedForkJoinPool.getForkJoinPool().submit(new Runnable() {
        @Override
        public void run() {
          for (long time = timescale.intervalSize + startTime; time <= startTime + incrementalStep + largestWindowSize; time += timescale.intervalSize) {
            final long start = time - timescale.windowSize;
            try {
              final Node<T> parent = finalTimespans.lookup(start, time);
              final List<Node<T>> childNodes = selectionAlgorithm.selection(start, time);
              for (final Node<T> elem : childNodes) {
                parent.addDependency(elem);
              }
            } catch (final NotFoundException e) {
              e.printStackTrace();
              throw new RuntimeException(e);
            }
          }
        }
      }));
    }

    // Join the tasks connecting edges.
    for (final ForkJoinTask task : taskEdges) {
      task.join();
    }
  }

  /**
   * Add DependencyGraphNode and connect dependent nodes.
   */
  private void addOverlappingWindowNodeAndEdge(final long curr, final long until) {
    final List<ForkJoinTask> tasks = new LinkedList<>();
    // Create nodes
    for (final Timescale timescale : timescales) {
      tasks.add(sharedForkJoinPool.getForkJoinPool().submit(new Runnable() {
        @Override
        public void run() {
          final long align = curr + (timescale.intervalSize - (curr - startTime) % timescale.intervalSize);
          for (long time = align; time <= until; time += timescale.intervalSize) {
            // create vertex and add it to the table cell of (time, windowsize)
            final long start = time - timescale.windowSize;
            Node parent;
            try {
              parent = finalTimespans.lookup(start, time);
            } catch (NotFoundException e) {
              parent = new Node(start, time, false);
              finalTimespans.saveOutput(start, time, parent);
            }
            LOG.log(Level.FINE, "Saved to table : (" + start + " , " + time + ") refCount : " + parent.refCnt);
          }
        }
      }));
    }

    // Join the tasks
    for (final ForkJoinTask task : tasks) {
      task.join();
    }

    final List<ForkJoinTask> taskEdges = new LinkedList<>();
    for (final Timescale timescale : timescales) {
      taskEdges.add(sharedForkJoinPool.getForkJoinPool().submit(new Runnable() {
        @Override
        public void run() {
          final long align = curr + (timescale.intervalSize - (curr - startTime) % timescale.intervalSize);
          for (long time = align; time <= until; time += timescale.intervalSize) {
            final long start = time - timescale.windowSize;
            final List<Node<T>> childNodes = selectionAlgorithm.selection(start, time);
            LOG.log(Level.FINE, "(" + start + ", " + time + ") dependencies1: " + childNodes);

            Node parent;
            try {
              parent = finalTimespans.lookup(start, time);
            } catch (NotFoundException e) {
              e.printStackTrace();
              throw new RuntimeException(e);
            }
            //System.out.println("ADD: " + start + "-" + time);
            //System.out.println("SAVE: [" + start + "-" + time + ")");
            for (final Node<T> elem : childNodes) {
              parent.addDependency(elem);
            }
            LOG.log(Level.FINE, "Saved to table : (" + start + " , " + time + ") refCount : " + parent.refCnt);
            LOG.log(Level.FINE, "(" + start + ", " + time + ") dependencies2: " + childNodes);
          }
        }
      }));
    }

    // Join the tasks
    for (final ForkJoinTask task : taskEdges) {
      task.join();
    }
  }

  @Override
  public List<Timespan> getFinalTimespans(final long t) {

    /**
     * [-------------------- period -----------------------]
     * 1)      ^       ^  <--------------->
     *         t      cbi        lw
     *
     * 2)              ^  <---------------> <----->
     *               t,cbi       lw          step
     *
     * 3)              ^ <----->  ^ <------------->
     *                 t   step  cbi     lw
     */
    final List<Timespan> timespans = new LinkedList<>();
    if (t >= currBuildingIndex && currBuildingIndex + largestWindowSize < period) {
      // Incremental build dependency graph
      addOverlappingWindowNodeAndEdge(currBuildingIndex + largestWindowSize,
          Math.min(period, currBuildingIndex + largestWindowSize + incrementalStep));
      currBuildingIndex += incrementalStep;
    }

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
