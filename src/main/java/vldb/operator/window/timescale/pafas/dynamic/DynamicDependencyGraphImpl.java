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
import vldb.operator.window.timescale.common.Timespan;
import vldb.operator.window.timescale.pafas.DependencyGraph;
import vldb.operator.window.timescale.pafas.Node;
import vldb.operator.window.timescale.pafas.PeriodCalculator;
import vldb.operator.window.timescale.parameter.StartTime;

import javax.inject.Inject;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * DependencyGraph shows which nodes are related to each other. This is used so that unnecessary outputs are not saved.
 */
public final class DynamicDependencyGraphImpl<T> implements DependencyGraph {

  private static final Logger LOG = Logger.getLogger(DynamicDependencyGraphImpl.class.getName());

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
  /**
   * DependencyGraph constructor. This first builds the dependency graph.
   * @param startTime the initial start time of when the graph is built.
   */
  @Inject
  private DynamicDependencyGraphImpl(@Parameter(StartTime.class) final long startTime,
                                     final DynamicPartialTimespans partialTimespans,
                                     final DynamicOutputLookupTable<Node<T>> outputLookupTable,
                                     final SelectionAlgorithm<T> selectionAlgorithm,
                                     final WindowManager windowManager,
                                     final TimeMonitor timeMonitor,
                                     final PeriodCalculator periodCalculator) {
    LOG.info("START " + this.getClass());
    this.windowManager = windowManager;
    this.timeMonitor = timeMonitor;
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
        try {
          parent = finalTimespans.lookup(start, time, timescale);
        } catch (NotFoundException e) {
          //System.out.println("ADD Node (" + start + ", " + time + ")" + ", curr: " + curr + ", until: " + until);
          parent = new Node(start, time, false);
          finalTimespans.saveOutput(start, time, timescale, parent);
          addedNodes.add(parent);
        }
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
    for (final Node parent : parentNodes) {
      //final List<Node<T>> childNodes = selectionAlgorithm.selection(Math.max(parent.start, startTime), parent.end);
      //System.out.println("FInd node: " + parent + ", " + parent.getDependencies());
      final List<Node<T>> childNodes = selectionAlgorithm.selection(parent.start, parent.end);
      LOG.log(Level.FINE, "(" + parent.start + ", " + parent.end + ") dependencies1: " + childNodes);
      //System.out.println("(" + parent.start + ", " + parent.end + ") dependencies1: " + childNodes);
      for (final Node<T> elem : childNodes) {
        parent.addDependency(elem);
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
    // Retrieve timespans which are created after addTime
    // ---------------|----------|
    //          window change
    //                Then, until currBuildingIndex, we rebuild the nodes.
    final List<Timespan> timespans = new LinkedList<>();
    for (long timespanET = addTime + 1; timespanET <= currBuildingIndex; timespanET++) {
      for (final Timescale timescale : timescales) {
        if (timescale != ts) {
          if ((timespanET - windowManager.timescaleStartTime(timescale)) % timescale.intervalSize == 0) {
            final Timespan newTS = new Timespan(Math.max(timespanET - timescale.windowSize, windowManager.timescaleStartTime(timescale)), timespanET, timescale);
            //System.out.println("newTS: " + newTS + ", " + newTS.timescale);
            timespans.add(newTS);
          }
        }
      }
    }

    // Remove nodes !!
    final List<Node<T>> prevChildNodes = new LinkedList<>();
    // Lookup and retrieve child nodes
    for (final Timespan timespan : timespans) {
      try {
        final Node<T> parentNode = finalTimespans.lookup(timespan.startTime, timespan.endTime, timespan.timescale);
        for (final Node<T> child : parentNode.getDependencies()) {
          // Decrease RefCnt
          child.refCnt -= 1;
          child.parents.remove(parentNode);
          prevChildNodes.add(child);
        }
        finalTimespans.deleteOutput(timespan.startTime, timespan.endTime, timespan.timescale);
      } catch (NotFoundException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }

    // Create new nodes and edges
    rebuildSize = windowManager.getRebuildSize();
    addOverlappingWindowNodeAndEdge(addTime, addTime + rebuildSize);

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


  public void removeSlidingWindow(final Timescale ts, final long tsStartTime, final long deleteTime) {
    // Retrieve timespans which are created after addTime
    // ---------------|----------|
    //          window change
    //                Then, until currBuildingIndex, we rebuild the nodes.
    final List<Timespan> timespans = new LinkedList<>();
    for (long timespanET = deleteTime + 1; timespanET <= currBuildingIndex; timespanET++) {
      for (final Timescale timescale : timescales) {
        if ((timespanET - windowManager.timescaleStartTime(timescale)) % timescale.intervalSize == 0) {
          final Timespan newTS = new Timespan(Math.max(timespanET - timescale.windowSize, windowManager.timescaleStartTime(timescale)), timespanET, timescale);
          //System.out.println("newTS: " + newTS + ", " + newTS.timescale);
          timespans.add(newTS);
        }
      }
    }
    for (long timespanET = deleteTime + 1; timespanET <= currBuildingIndex; timespanET++) {
      if ((timespanET - tsStartTime) % ts.intervalSize == 0) {
        final Timespan newTS = new Timespan(Math.max(timespanET - ts.windowSize, tsStartTime), timespanET, ts);
        //System.out.println("newTS: " + newTS + ", " + newTS.timescale);
        timespans.add(newTS);
      }
    }

    // Remove nodes !!
    final List<Node<T>> prevChildNodes = new LinkedList<>();
    // Lookup and retrieve child nodes
    for (final Timespan timespan : timespans) {
      try {
        final Node<T> parentNode = finalTimespans.lookup(timespan.startTime, timespan.endTime, timespan.timescale);
        for (final Node<T> child : parentNode.getDependencies()) {
          // Decrease RefCnt
          child.refCnt -= 1;
          child.parents.remove(parentNode);
          prevChildNodes.add(child);
        }
        finalTimespans.deleteOutput(timespan.startTime, timespan.endTime, timespan.timescale);
      } catch (NotFoundException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }
    //System.out.println("After final remove: " + partialTimespans);

    // Create new nodes and edges
    rebuildSize = windowManager.getRebuildSize();
    addOverlappingWindowNodeAndEdge(deleteTime, deleteTime + rebuildSize);

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

    /*
    // Retrieve timespans that will be deleted
    final List<Timespan> timespans = new LinkedList<>();
    for (long timespanET = deleteTime + 1; timespanET <= currBuildingIndex; timespanET++) {
      if ((timespanET - tsStartTime) % timescale.intervalSize == 0) {
        final Timespan newTS = new Timespan(Math.max(timespanET - timescale.windowSize, tsStartTime), timespanET, timescale);
        //System.out.println("newTS: " + newTS + ", " + newTS.timescale);
        timespans.add(newTS);
      }
    }

    // Remove nodes !!
    final List<Node<T>> prevChildNodes = new LinkedList<>();
    final List<Node<T>> removedNodes = new LinkedList<>();
    // Lookup and retrieve child nodes
    for (final Timespan timespan : timespans) {
      try {
        final Node<T> parentNode = finalTimespans.lookup(timespan.startTime, timespan.endTime, timespan.timescale);
        removedNodes.add(parentNode);
        for (final Node<T> child : parentNode.getDependencies()) {
          // Decrease RefCnt
          child.refCnt -= 1;
          prevChildNodes.add(child);
        }
        finalTimespans.deleteOutput(timespan.startTime, timespan.endTime, timespan.timescale);
      } catch (NotFoundException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }

    // Re-connect parents!
    final Set<Node<T>> parents = new HashSet<>();
    for (final Node<T> removedNode : removedNodes) {
      final List<Node<T>> nodes = removedNode.parents;
      for (final Node<T> pnode : nodes) {
        for (final Node<T> cNode : pnode.getDependencies()) {
          cNode.refCnt -= 1;
        }
      }
      parents.addAll(removedNode.parents);
    }
    System.out.println("PARENTS: " + parents);
    addEdges(parents);

    // Remove unused child nodes
    // Remove child nodes which have zero reference count
    for (final Node<T> child : prevChildNodes) {
      if (child.refCnt == 0) {
        if (child.end <= deleteTime) {
          if (child.partial) {
            partialTimespans.removeNode(child.start);
          } else {
            finalTimespans.deleteOutput(child.start, child.end, child);
          }
        }
      }
    }
    */
  }

  public void removeNode(final Node<T> node) {
    finalTimespans.deleteOutput(node.start, node.end, node);
  }

}
