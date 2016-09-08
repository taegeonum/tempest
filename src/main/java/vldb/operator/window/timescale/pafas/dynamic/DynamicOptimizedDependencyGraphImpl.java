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

package vldb.operator.window.timescale.pafas.dynamic;

import org.apache.reef.tang.annotations.Parameter;
import vldb.operator.common.NotFoundException;
import vldb.operator.window.timescale.TimeMonitor;
import vldb.operator.window.timescale.Timescale;
import vldb.operator.window.timescale.common.SharedWorkStealingPool;
import vldb.operator.window.timescale.common.Timespan;
import vldb.operator.window.timescale.pafas.Node;
import vldb.operator.window.timescale.pafas.PeriodCalculator;
import vldb.operator.window.timescale.parameter.NumThreads;
import vldb.operator.window.timescale.parameter.StartTime;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * DependencyGraph shows which nodes are related to each other. This is used so that unnecessary outputs are not saved.
 */
public final class DynamicOptimizedDependencyGraphImpl<T> implements DynamicDependencyGraph<T> {

  private static final Logger LOG = Logger.getLogger(DynamicOptimizedDependencyGraphImpl.class.getName());

  /**
   * A table containing DependencyGraphNode for partial outputs.
   */
  //private final DefaultOutputLookupTableImpl<DependencyGraphNode> partialOutputTable;

  /**
   * The list of timescale.
   */
  private final List<Timescale> timescales;

  /**
   * A start time.
   */
  private final long startTime;

  private final DynamicPartialTimespans<T> partialTimespans;
  private final DynamicOutputLookupTable<Node<T>> finalTimespans;
  private final SelectionAlgorithm<T> selectionAlgorithm;

  private long currBuildingIndex;
  private final long period;
  private long rebuildSize;
  private final WindowManager windowManager;

  private final TimeMonitor timeMonitor;
  private final ExecutorService workStealingPool;
  /**
   * DependencyGraph constructor. This first builds the dependency graph.
   * @param startTime the initial start time of when the graph is built.
   */
  @Inject
  private DynamicOptimizedDependencyGraphImpl(@Parameter(StartTime.class) final long startTime,
                                              final DynamicPartialTimespans partialTimespans,
                                              final DynamicOutputLookupTable<Node<T>> outputLookupTable,
                                              final SelectionAlgorithm<T> selectionAlgorithm,
                                              final WindowManager windowManager,
                                              final TimeMonitor timeMonitor,
                                              final SharedWorkStealingPool sharedWorkStealingPool,
                                              @Parameter(NumThreads.class) final int numThreads,
             final PeriodCalculator periodCalculator) {
    LOG.info("START " + this.getClass());
    this.windowManager = windowManager;
    this.timeMonitor = timeMonitor;
    this.workStealingPool = sharedWorkStealingPool.getWorkStealingPool();
    this.partialTimespans = partialTimespans;
    this.timescales = windowManager.timescales;
    this.startTime = startTime;
    this.currBuildingIndex = startTime + windowManager.getRebuildSize();
    this.rebuildSize = windowManager.getRebuildSize();
    this.finalTimespans = outputLookupTable;
    this.selectionAlgorithm = selectionAlgorithm;
    this.period = periodCalculator.getPeriod();
    addOverlappingWindowNodeAndEdge(startTime, startTime + rebuildSize);
    //System.out.println("End of DynamicDependencyGraphImpl");
  }

  /**
   * Add DependencyGraphNode and connect dependent nodes.
   */
  private void addOverlappingWindowNodeAndEdge(final long curr, final long until) {
    //System.out.println("curr: " + curr + ", until: " + until);
    final List<Node<T>> addedNodes = new LinkedList<>();
    currBuildingIndex = until;
    for (final Timescale timescale : timescales) {
      final long align = curr + (timescale.intervalSize - (curr - windowManager.timescaleStartTime(timescale)) % timescale.intervalSize);
      for (long time = align; time <= until; time += timescale.intervalSize) {
        // create vertex and add it to the table cell of (time, windowsize)
        final long start = Math.max(time - timescale.windowSize, windowManager.timescaleStartTime(timescale));
        //System.out.println("aa: (" + start + ", " + time + "), " + "curr: " + curr + ", until: " + until + ", ts: " + timescale);
        Node<T> parent;
          //System.out.println("ADD Node (" + start + ", " + time + ")" + ", curr: " + curr + ", until: " + until);
        parent = new Node(start, time, false);
        finalTimespans.saveOutput(start, time, timescale, parent);
        addedNodes.add(parent);
        //System.out.println("ADD: " + start + "-" + time);
        //System.out.println("SAVE: [" + start + "-" + time + ")");

        LOG.log(Level.FINE, "Saved to table : (" + start + " , " + time + ") refCount : " + parent.refCnt);
      }
    }

    // Add edges
    addEdges(addedNodes);
  }

  private void addEdges(final Collection<Node<T>> parentNodes) {
    // Add edges
    final List<Future> tasks = new LinkedList<>();

    for (final Node parent : parentNodes) {
      //final List<Node<T>> childNodes = selectionAlgorithm.selection(Math.max(parent.start, startTime), parent.end);
      //System.out.println("FInd node: " + parent + ", " + parent.getDependencies());
      tasks.add(workStealingPool.submit(new Runnable() {
        @Override
        public void run() {
          final List<Node<T>> childNodes = selectionAlgorithm.selection(parent.start, parent.end);
          LOG.log(Level.FINE, "(" + parent.start + ", " + parent.end + ") dependencies1: " + childNodes);
          //System.out.println("(" + parent.start + ", " + parent.end + ") dependencies1: " + childNodes);
          for (final Node<T> elem : childNodes) {
            parent.addDependency(elem);
          }
        }
      }));
    }

    for (final Future task : tasks) {
      try {
        task.get();
      } catch (InterruptedException e) {
        e.printStackTrace();
      } catch (ExecutionException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
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
    //System.out.println("GET_FINAL: " + t + ", rebuildSize: " + rebuildSize + ", currBuildingIndex; " + currBuildingIndex);
    if (t + rebuildSize > currBuildingIndex) {
      // Incremental build dependency graph
      final long s = System.nanoTime();
      addOverlappingWindowNodeAndEdge(currBuildingIndex, t + rebuildSize);
      final long e = System.nanoTime();
      timeMonitor.continuousTime += (e-s);
      currBuildingIndex = t + rebuildSize;
    }

    for (final Timescale timescale : timescales) {
      if (t - windowManager.timescaleStartTime(timescale) != 0 && (t - windowManager.timescaleStartTime(timescale)) % timescale.intervalSize == 0) {
        final long st = Math.max(t - timescale.windowSize, windowManager.timescaleStartTime(timescale));
        timespans.add(new Timespan(Math.max(t - timescale.windowSize, windowManager.timescaleStartTime(timescale)), t, timescale));
      }
    }
    return timespans;
  }

  @Override
  public Node<T> getNode(final Timespan timespan) {
    long adjStartTime = timespan.startTime;
    final long adjEndTime = timespan.endTime;
    //System.out.println("ORIGIN: " + timespan + ", ADJ: [" + adjStartTime + ", " + adjEndTime + ")");

    if (timespan.timescale == null) {
      final Node<T> partialTimespanNode = partialTimespans.getNextPartialTimespanNode(adjStartTime);
      if (partialTimespanNode.end != adjEndTime) {
        throw new RuntimeException("No partial: " + timespan);
      }
      return partialTimespanNode;
    }

    try {
      return finalTimespans.lookup(adjStartTime, adjEndTime, timespan.timescale);
    } catch (NotFoundException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public long getPeriod() {
    return period;
  }

  @Override
  public void addSlidingWindow(final Timescale ts, final long addTime) {
    // Possible timespans to be changed
    final List<Timespan> timespans = new LinkedList<>();
    for (final Timescale timescale : timescales) {
      if (timescale.windowSize > ts.windowSize) {
        final long st = addTime - (addTime - windowManager.timescaleStartTime(timescale)) % timescale.intervalSize + timescale.intervalSize;
        for (long timespanET = st; timespanET <= currBuildingIndex; timespanET += timescale.intervalSize) {
          final long timespanST = Math.max(timespanET - timescale.windowSize, windowManager.timescaleStartTime(timescale));
          if (timespanET - timespanST > ts.windowSize) {
            final Timespan newTS = new Timespan(timespanST, timespanET, timescale);
            timespans.add(newTS);
          }
        }
      }
    }

    //System.out.println("Possible delete nodes: " + timespans);

    // Remove nodes !!
    final List<Node<T>> prevChildNodes = new LinkedList<>();

    // Node to find edges
    final List<Node<T>> findEdgeNodes = new LinkedList<>();
    // Lookup and retrieve child nodes
    for (final Timespan timespan : timespans) {
      try {
        final Node<T> parentNode = finalTimespans.lookup(timespan.startTime, timespan.endTime, timespan.timescale);
        findEdgeNodes.add(parentNode);
        for (final Node<T> child : parentNode.getDependencies()) {
          // Decrease RefCnt
          child.refCnt -= 1;
          child.parents.remove(parentNode);
          prevChildNodes.add(child);
        }
        parentNode.getDependencies().clear();
        //finalTimespans.deleteOutput(timespan.startTime, timespan.endTime, timespan.timescale);
      } catch (NotFoundException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }

    // Add new nodes for the new timescale
    final long st = addTime + ts.intervalSize;
    for (long timespanET = st; timespanET <= currBuildingIndex; timespanET += ts.intervalSize) {
      final long start_ts = Math.max(timespanET - ts.windowSize, addTime);
      final long end_ts = timespanET;
      final Node<T> node =  new Node<T>(start_ts, end_ts, false);
      findEdgeNodes.add(node);
      finalTimespans.saveOutput(Math.max(timespanET - ts.windowSize, addTime), timespanET, ts, node);
    }

    // Create edges
    addEdges(findEdgeNodes);

    // Build unconstructed area
    final long prevRebuildSize = rebuildSize;
    rebuildSize = windowManager.getRebuildSize();

    if (currBuildingIndex < addTime + rebuildSize) {
      addOverlappingWindowNodeAndEdge(currBuildingIndex, addTime + rebuildSize);
      currBuildingIndex = addTime + rebuildSize;
    }

    // Remove child nodes which have zero reference count
    for (final Node<T> child : prevChildNodes) {
      if (child.refCnt == 0) {
        if (child.end <= addTime) {
          if (child.partial) {
            partialTimespans.removeNode(child.start);
          } else {
            finalTimespans.deleteOutput(child.start, child.end, child);
          }
        }
      }
    }
  }

  @Override
  public void removeSlidingWindow(final Timescale timescale, final long deleteTime) {
    throw new RuntimeException("not implemeted");
  }

  @Override
  public void removeSlidingWindow(final Timescale ts, final long tsStartTime, final long deleteTime) {
    final long prevRebuildSize = rebuildSize;
    rebuildSize = windowManager.getRebuildSize();

    // Remove nodes !!
    final List<Node<T>> prevChildNodes = new LinkedList<>();

    // Shrink the size of DAG!!
    if (rebuildSize + deleteTime < currBuildingIndex) {
      //System.out.println("Shrink: " + currBuildingIndex + ", " + (rebuildSize+deleteTime));
      // Remove rebuild - prevRebuild
      final List<Timespan> timespans = new LinkedList<>();
      for (long timespanET = deleteTime + rebuildSize + 1; timespanET <= currBuildingIndex; timespanET++) {
        for (final Timescale timescale : timescales) {
          if ((timespanET - windowManager.timescaleStartTime(timescale)) % timescale.intervalSize == 0) {
            final Timespan newTS = new Timespan(Math.max(timespanET - timescale.windowSize, windowManager.timescaleStartTime(timescale)), timespanET, timescale);
            //System.out.println("newTS: " + newTS + ", " + newTS.timescale);
            timespans.add(newTS);
          }
        }
      }
      for (long timespanET = deleteTime + rebuildSize  + 1; timespanET <= currBuildingIndex; timespanET++) {
        if ((timespanET - tsStartTime) % ts.intervalSize == 0) {
          final Timespan newTS = new Timespan(Math.max(timespanET - ts.windowSize, tsStartTime), timespanET, ts);
          //System.out.println("newTS: " + newTS + ", " + newTS.timescale);
          timespans.add(newTS);
        }
      }

      // Remove the timespans
      // Lookup and retrieve child nodes
      for (final Timespan timespan : timespans) {
        try {
          final Node<T> deleteNode = finalTimespans.lookup(timespan.startTime, timespan.endTime, timespan.timescale);
          for (final Node<T> child : deleteNode.getDependencies()) {
            // Decrease RefCnt
            child.refCnt -= 1;
            child.parents.remove(deleteNode);
            prevChildNodes.add(child);
          }
          finalTimespans.deleteOutput(timespan.startTime, timespan.endTime, timespan.timescale);
        } catch (NotFoundException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      }

      // Re-set current building index
      currBuildingIndex = deleteTime + rebuildSize;
    }

    // After that, re-connect the edges of parents nodes of the deleted nodes.
    final List<Timespan> timespans = new LinkedList<>();
    for (long timespanET = deleteTime + 1; timespanET <= currBuildingIndex; timespanET++) {
      if ((timespanET - tsStartTime) % ts.intervalSize == 0) {
        final Timespan newTS = new Timespan(Math.max(timespanET - ts.windowSize, tsStartTime), timespanET, ts);
        //System.out.println("newTS: " + newTS + ", " + newTS.timescale);
        timespans.add(newTS);
      }
    }

    final Set<Node<T>> findingEdgeNodes = new HashSet<>();
    final Set<Node<T>> deleteNodes = new HashSet<>();
    // Lookup and retrieve child nodes
    for (final Timespan timespan : timespans) {
      try {
        final Node<T> deleteNode = finalTimespans.lookup(timespan.startTime, timespan.endTime, timespan.timescale);
        deleteNodes.add(deleteNode);
        // Add parent nodes

        findingEdgeNodes.addAll(deleteNode.parents);
        for (final Node<T> child : deleteNode.getDependencies()) {
          // Decrease RefCnt
          child.refCnt -= 1;
          child.parents.remove(deleteNode);
          prevChildNodes.add(child);
        }
        finalTimespans.deleteOutput(timespan.startTime, timespan.endTime, timespan.timescale);
      } catch (NotFoundException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }

    findingEdgeNodes.removeAll(deleteNodes);
    // Remove childs of the changing nodes
    for (final Node<T> parentNode : findingEdgeNodes) {
      for (final Node<T> child : parentNode.getDependencies()) {
        // Decrease RefCnt
        child.refCnt -= 1;
        child.parents.remove(parentNode);
        prevChildNodes.add(child);
      }
      parentNode.getDependencies().clear();
    }

    //System.out.println("findingEdgeNode: " + findingEdgeNodes);
    // Create edges
    addEdges(findingEdgeNodes);

    // Remove child nodes which have zero reference count
    for (final Node<T> child : prevChildNodes) {
      if (child.refCnt == 0) {
        if (child.end <= deleteTime) {
          //System.out.println("Delete: " + child);
          if (child.partial) {
            partialTimespans.removeNode(child.start);
          } else {
            finalTimespans.deleteOutput(child.start, child.end, child);
          }
        }
      }
    }
  }

  @Override
  public void removeNode(final Node<T> node) {
    finalTimespans.deleteOutput(node.start, node.end, node);
  }

}
