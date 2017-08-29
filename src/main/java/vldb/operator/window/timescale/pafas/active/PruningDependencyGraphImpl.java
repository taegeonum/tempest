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

package vldb.operator.window.timescale.pafas.active;

import org.apache.reef.tang.annotations.Parameter;
import vldb.operator.common.NotFoundException;
import vldb.operator.window.timescale.Timescale;
import vldb.operator.window.timescale.common.OutputLookupTable;
import vldb.operator.window.timescale.common.TimescaleParser;
import vldb.operator.window.timescale.common.Timespan;
import vldb.operator.window.timescale.pafas.DependencyGraph;
import vldb.operator.window.timescale.pafas.Node;
import vldb.operator.window.timescale.pafas.PartialTimespans;
import vldb.operator.window.timescale.pafas.PeriodCalculator;
import vldb.operator.window.timescale.parameter.NumThreads;
import vldb.operator.window.timescale.parameter.ReusingRatio;
import vldb.operator.window.timescale.parameter.StartTime;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * DependencyGraph shows which nodes are related to each other. This is used so that unnecessary outputs are not saved.
 */
public final class PruningDependencyGraphImpl<T> implements DependencyGraph {

  private static final Logger LOG = Logger.getLogger(PruningDependencyGraphImpl.class.getName());

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
  private final int numThreads;
  private final double reusingRatio;

  private final Set<Node<T>> noSharedNode = new HashSet<>();

  /**
   * DependencyGraph constructor. This first builds the dependency graph.
   * @param startTime the initial start time of when the graph is built.
   */
  @Inject
  private PruningDependencyGraphImpl(final TimescaleParser tsParser,
                                     @Parameter(StartTime.class) final long startTime,
                                     final PartialTimespans partialTimespans,
                                     final PeriodCalculator periodCalculator,
                                     final OutputLookupTable<Node<T>> outputLookupTable,
                                     @Parameter(NumThreads.class) final int numThreads,
                                     @Parameter(ReusingRatio.class) final double reusingRatio,
                                     final SelectionAlgorithm<T> selectionAlgorithm) {
    this.partialTimespans = partialTimespans;
    this.timescales = tsParser.timescales;
    this.period = periodCalculator.getPeriod();
    this.startTime = startTime;
    this.finalTimespans = outputLookupTable;
    this.numThreads = numThreads;
    this.reusingRatio = reusingRatio;
    this.selectionAlgorithm = selectionAlgorithm;

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

  private void pruning(final List<Node<T>> addedNodes) {

    final Node<T>[] array = new Node[addedNodes.size()];
    for (int i = 0; i < array.length; i++) {
      array[i] = addedNodes.get(i);
    }

    Arrays.parallelSort(array, new Comparator<Node<T>>() {
      @Override
      public int compare(final Node<T> o1, final Node<T> o2) {
        final long windowSize1 = o1.end - o1.start;
        final long windowSize2 = o2.end - o2.start;

        if (o1.start < o2.start) {
          if (windowSize1 < windowSize2) {
            return -1;
          } else if (windowSize1 > windowSize2) {
            return 1;
          } else {
            return 0;
          }
        } else if (o1.start > o2.start) {
          return 1;
        } else {
          return 0;
        }
      }
    });

    // Counting the possible reference count!
    for (int i = 0; i < array.length; i++) {
      final Node<T> node = array[i];
      final long windowSize1 = node.end - node.start;
      int count = 0;
      for (int j = i+1; j < array.length; j++) {
        final Node<T> possibleParent = array[j];
        final long windowSize2 = possibleParent.end - possibleParent.start;
        if (possibleParent.start == node.start && (windowSize2 > windowSize1)) {
          count += 1;
        } else {
          break;
        }
      }
      node.possibleParentCount = count;
    }

    // Sorting by possible parent count
    Arrays.parallelSort(array, new Comparator<Node<T>>() {
      @Override
      public int compare(final Node<T> o1, final Node<T> o2) {
        if (o1.possibleParentCount < o2.possibleParentCount) {
          return -1;
        } else if (o1.possibleParentCount > o2.possibleParentCount) {
          return 1;
        } else {
          return 0;
        }
      }
    });

    // Pruning!!
    final int pruningIndex = Math.min(array.length-1, (int)(reusingRatio * array.length));
    for (int i = pruningIndex; i < array.length; i++) {
      array[i].isNotShared = true;
    }
  }


  /**
   * Add DependencyGraphNode and connect dependent nodes.
   */
  private void addOverlappingWindowNodeAndEdge() {
    final ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
    final List<Node<T>> addedNodes = new LinkedList<>();

    for (final Timescale timescale : timescales) {

      for (long time = timescale.intervalSize + startTime; time <= period + startTime; time += timescale.intervalSize) {
        // create vertex and add it to the table cell of (time, windowsize)
        final long start = time - timescale.windowSize;
        final Node parent = new Node(start, time, false);
        addedNodes.add(parent);

        finalTimespans.saveOutput(start, time, parent);
        LOG.log(Level.FINE, "Saved to table : (" + start + " , " + time + ") refCount : " + parent.refCnt);
        //LOG.log(Level.FINE, "(" + start + ", " + time + ") dependencies2: " + childNodes);
      }
    }

    if (reusingRatio < 1.0) {
      pruning(addedNodes);
    }

    // Add edges
    for (final Node node : addedNodes) {
      executorService.submit(() -> {
        addEdge(node);
      });
    }

    executorService.shutdown();
    try {
      executorService.awaitTermination(1000, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  private void addEdge(final Node<T> parent) {
    //System.out.println("child node start: " + parent);
    final List<Node<T>> childNodes = selectionAlgorithm.selection(parent.start, parent.end);
    //System.out.println("child node end: " + parent);
    LOG.log(Level.FINE, "(" + parent.start + ", " + parent.end + ") dependencies1: " + childNodes);
    //System.out.println("(" + parent.start + ", " + parent.end + ") dependencies1: " + childNodes);
    for (final Node<T> elem : childNodes) {
      parent.addDependency(elem);
    }
  }

  private void addEdges(final Collection<Node<T>> parentNodes) {
    // Add edges
    //final List<Future> futures = new LinkedList<>();

    for (final Node parent : parentNodes) {
      addEdge(parent);
    }
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

    if (timespan.timescale == null) {
      final Node<T> partialTimespanNode = partialTimespans.getNextPartialTimespanNode(adjStartTime);
      if (partialTimespanNode.end != adjEndTime) {
        throw new RuntimeException("not found");
      }
      return partialTimespanNode;
    }

    try {
      return finalTimespans.lookup(adjStartTime, adjEndTime);
    } catch (final NotFoundException e) {
      // find partial timespan
      throw new RuntimeException(e);
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
