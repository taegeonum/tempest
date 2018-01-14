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

package vldb.operator.window.timescale.pafas.vldb2018.dynamic;

import org.apache.reef.tang.annotations.Parameter;
import vldb.operator.common.NotFoundException;
import vldb.operator.window.timescale.TimeMonitor;
import vldb.operator.window.timescale.Timescale;
import vldb.operator.window.timescale.common.OutputLookupTable;
import vldb.operator.window.timescale.common.Timespan;
import vldb.operator.window.timescale.pafas.Node;
import vldb.operator.window.timescale.pafas.PeriodCalculator;
import vldb.operator.window.timescale.pafas.dynamic.DynamicDependencyGraph;
import vldb.operator.window.timescale.pafas.dynamic.DynamicPartialTimespans;
import vldb.operator.window.timescale.pafas.dynamic.WindowManager;
import vldb.operator.window.timescale.parameter.StartTime;

import javax.inject.Inject;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * DependencyGraph shows which nodes are related to each other. This is used so that unnecessary outputs are not saved.
 */
public final class DynamicFlatFitCombinedDependencyGraphImpl<T> implements DynamicDependencyGraph<T> {

  private static final Logger LOG = Logger.getLogger(DynamicFlatFitCombinedDependencyGraphImpl.class.getName());

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
  private final OutputLookupTable<Node<T>> finalTimespans;
  private final SelectionAlgorithm<T> selectionAlgorithm;

  private long currBuildingIndex;
  private final long period;
  private long rebuildSize;
  private final WindowManager windowManager;

  private final TimeMonitor timeMonitor;


  private final List<Integer> pointers;

  private final Stack<Integer> positions;

  private final int wSize;

  private int currInd;

  private int prevInd;

  private final Map<Timescale, Integer> wSizeMap;
  private final List<Timespan> timespans;

  /**
   * DependencyGraph constructor. This first builds the dependency graph.
   * @param startTime the initial start time of when the graph is built.
   */
  @Inject
  private DynamicFlatFitCombinedDependencyGraphImpl(@Parameter(StartTime.class) final long startTime,
                                                    final DynamicPartialTimespans partialTimespans,
                                                    final OutputLookupTable<Node<T>> outputLookupTable,
                                                    final SelectionAlgorithm<T> selectionAlgorithm,
                                                    final WindowManager windowManager,
                                                    final TimeMonitor timeMonitor,
                                                    final PeriodCalculator periodCalculator) {
    LOG.info("START " + this.getClass());
    this.windowManager = windowManager;
    this.timeMonitor = timeMonitor;
    this.wSizeMap = new HashMap<>();
    this.partialTimespans = partialTimespans;
    this.timescales = windowManager.timescales;
    this.startTime = startTime;
    this.currBuildingIndex = startTime + windowManager.getRebuildSize();
    this.rebuildSize = windowManager.getRebuildSize();
    this.finalTimespans = outputLookupTable;
    this.wSize = findWsize(windowManager.timescales);
    this.selectionAlgorithm = selectionAlgorithm;
    this.pointers = new ArrayList<>(wSize);
    this.positions = new Stack<>();
    this.period = periodCalculator.getPeriod();
    this.timespans = new ArrayList<>(wSize);


    // Initialize
    for (int i = 0; i < wSize; i++) {
      pointers.add(i + 1);
      timespans.add(new Timespan(0, 0, null));
    }
    pointers.set(wSize - 1, 0);
    currInd = 0;
    prevInd = wSize - 1;

    addOverlappingWindowNodeAndEdge(startTime, startTime + rebuildSize, true);
    //System.out.println("End of DynamicDependencyGraphImpl");
  }


  private int findWsize(final List<Timescale> timescales) {
    int max = 0;
    for (final Timescale timescale : timescales) {
      int count = 0;
      long t = startTime;
      while ((t = partialTimespans.getNextSliceTime(t)) <= timescale.windowSize + startTime) {
        count += 1;
      }
      wSizeMap.put(timescale, count);

      if (max < count) {
        max = count;
      }
    }

    return max;
  }


  private final List<Timescale> getWindows(final long time) {
    final List<Timescale> endWindows = new LinkedList<>();
    for (int i = windowManager.timescales.size() - 1; i >= 0; i--) {
      final Timescale timescale = windowManager.timescales.get(i);
      if ((time - startTime) % timescale.intervalSize == 0) {
        endWindows.add(timescale);
      }
    }
    return endWindows;
  }

  private Timespan mergeTs(final Timespan ts1, final Timespan ts2) {
    if (ts1.startTime < ts2.startTime) {
      return new Timespan(ts1.startTime, ts2.endTime, null);
    } else {
      return new Timespan(ts2.startTime, ts1.endTime, null);
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
      if (elem.intermediate && elem.getDependencies().size() == 0) {
        addEdge(elem);
      }
    }
  }

  /**
   * Add DependencyGraphNode and connect dependent nodes.
   */
  private void addOverlappingWindowNodeAndEdge(final long curr, final long until, final boolean isStart) {
    //System.out.println("curr: " + curr + ", until: " + until);
    final List<Node<T>> addedNodes = new LinkedList<>();
    currBuildingIndex = until;
    for (final Timescale timescale : timescales) {
      final long align = curr + (timescale.intervalSize
          - (curr - windowManager.timescaleStartTime(timescale)) % timescale.intervalSize);
      for (long time = align; time <= until; time += timescale.intervalSize) {
        // create vertex and add it to the table cell of (time, windowsize)
        final long start = Math.max(time - timescale.windowSize, windowManager.timescaleStartTime(timescale));
        //System.out.println("aa: (" + start + ", " + time + "), " + "curr: " + curr + ", until: " + until + ", ts: " + timescale);
        Node<T> parent;
        try {
          parent = finalTimespans.lookup(start, time);
          parent.timescales.add(timescale);
        } catch (final NotFoundException e) {
          //System.out.println("ADD Node (" + start + ", " + time + ")" + ", curr: " + curr + ", until: " + until);
          parent = new Node(start, time, false, timescale);
          finalTimespans.saveOutput(start, time, parent);
          addedNodes.add(parent);
        }
        //System.out.println("ADD: " + start + "-" + time);
        //System.out.println("SAVE: [" + start + "-" + time + ")");

        LOG.log(Level.FINE, "Saved to table : (" + start + " , " + time + ") refCount : " + parent.refCnt);
      }
    }

    // Add Inter nodes
    final List<Node<T>> interNodes = new LinkedList<>();

    for (long tickTime = curr + 1; tickTime <= until; tickTime++) {
      pointers.set(prevInd, currInd);
      timespans.set(prevInd, new Timespan(tickTime - 1, tickTime, null));

      // get final timespans
      final List<Timescale> queriesToAnswer = getWindows(tickTime);
      for (final Timescale query : queriesToAnswer) {

        int startInd = currInd - wSizeMap.get(query);
        if (startInd < 0) {
          startInd += wSize;
        }

        do {
          positions.push(startInd);
          startInd = pointers.get(startInd);
        } while (startInd != currInd);

        final int ind = positions.pop();
        final Timespan ts = timespans.get(ind);

        while (positions.size() > 1) {
          final int tempInd = positions.pop();
          final Timespan tempTs = timespans.get(tempInd);

          pointers.set(tempInd, currInd);

          final Timespan newTs = mergeTs(ts, tempTs);
          //System.out.println("Store "  + newTs +
          //    ", for [" + (tickTime - query.windowSize) + ", " + tickTime + ")");

          timespans.set(tempInd, newTs);

          if (newTs.startTime != 0 && newTs.endTime != 0
              && (newTs.endTime - newTs.startTime) > 1) {
            // Add intermediate
            try {
              finalTimespans.lookup(newTs.startTime, newTs.endTime);
            } catch (final NotFoundException e) {
              final Node<T> newNode = new Node<>(newTs.startTime, newTs.endTime, false);
              newNode.intermediate = true;
              finalTimespans.saveOutput(newTs.startTime, newTs.endTime, newNode);
              interNodes.add(newNode);
            }
          }
        }

        if (positions.size() > 0) {
          final int uu = positions.pop();
        }
      }

      prevInd = currInd;
      currInd += 1;
      if (currInd == wSize) {
        currInd = 0;
      }
    }
    // Add inter node end

    // Add edges
    addEdges(addedNodes);


    // Remove unnecessary intermediate nodes
    interNodes.stream().forEach(node -> {
      if (node.refCnt.get() == 0 && node.intermediate) {
        finalTimespans.deleteOutput(node.start, node.end);
      }
    });

    // Adjust partial nodes!
    //adjustPartialNodes(until - rebuildSize, until);

    //System.out.println("Build end");
  }

  private void addEdges(final Collection<Node<T>> parentNodes) {
    // Add edges
    for (final Node<T> parent : parentNodes) {
      addEdge(parent);
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
    //System.out.println("GET_FINAL: " + t + ", rebuildSize: " + rebuildSize + ", currBuildingIndex; " + currBuildingIndex);
    if (t + rebuildSize > currBuildingIndex) {
      // Incremental build dependency graph
      final long s = System.nanoTime();
      addOverlappingWindowNodeAndEdge(currBuildingIndex, t + rebuildSize, false);
      final long e = System.nanoTime();
      timeMonitor.continuousTime += (e-s);
      currBuildingIndex = t + rebuildSize;
    }

    return null;
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
      return finalTimespans.lookup(adjStartTime, adjEndTime);
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
        final Node<T> parentNode = finalTimespans.lookup(timespan.startTime, timespan.endTime);
        for (final Node<T> child : parentNode.getDependencies()) {
          // Decrease RefCnt
          child.refCnt.decrementAndGet();
          child.parents.remove(parentNode);
          prevChildNodes.add(child);
        }
        finalTimespans.deleteOutput(timespan.startTime, timespan.endTime);
      } catch (NotFoundException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }

    // Create new nodes and edges
    rebuildSize = windowManager.getRebuildSize();
    addOverlappingWindowNodeAndEdge(addTime, addTime + rebuildSize, false);

    // Remove child nodes which have zero reference count
    for (final Node<T> child : prevChildNodes) {
      if (child.refCnt.get() == 0) {
        if (child.end <= addTime) {
          if (child.partial) {
            partialTimespans.removeNode(child.start);
          } else {
            finalTimespans.deleteOutput(child.start, child.end);
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
        final Node<T> parentNode = finalTimespans.lookup(timespan.startTime, timespan.endTime);
        for (final Node<T> child : parentNode.getDependencies()) {
          // Decrease RefCnt
          child.refCnt.decrementAndGet();
          child.parents.remove(parentNode);
          prevChildNodes.add(child);
        }
        finalTimespans.deleteOutput(timespan.startTime, timespan.endTime);
      } catch (NotFoundException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }
    //System.out.println("After final remove: " + partialTimespans);

    // Create new nodes and edges
    rebuildSize = windowManager.getRebuildSize();
    addOverlappingWindowNodeAndEdge(deleteTime, deleteTime + rebuildSize, false);

    // Remove child nodes which have zero reference count
    for (final Node<T> child : prevChildNodes) {
      if (child.refCnt.get() == 0) {
        if (child.end <= deleteTime) {
          //System.out.println("Delete: " + child);
          if (child.partial) {
            partialTimespans.removeNode(child.start);
          } else {
            finalTimespans.deleteOutput(child.start, child.end);
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

  @Override
  public void removeNode(final Node<T> node) {
    finalTimespans.deleteOutput(node.start, node.end);
  }

}
