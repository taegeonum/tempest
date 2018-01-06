
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

package vldb.operator.window.timescale.pafas.vldb2018.multithread;

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
import vldb.operator.window.timescale.pafas.dynamic.WindowManager;
import vldb.operator.window.timescale.parameter.NumThreads;
import vldb.operator.window.timescale.parameter.OverlappingRatio;
import vldb.operator.window.timescale.parameter.StartTime;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * DependencyGraph shows which nodes are related to each other. This is used so that unnecessary outputs are not saved.
 */
public final class SimpleTreeHeightDependencyGraph<T> implements DependencyGraph {

  private static final Logger LOG = Logger.getLogger(SimpleTreeHeightDependencyGraph.class.getName());

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

  private final WindowManager windowManager;
  private final int largestWindowSize;

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
  private SimpleTreeHeightDependencyGraph(final TimescaleParser tsParser,
                                          @Parameter(StartTime.class) final long startTime,
                                          final PartialTimespans partialTimespans,
                                          final PeriodCalculator periodCalculator,
                                          final OutputLookupTable<Node<T>> outputLookupTable,
                                          @Parameter(NumThreads.class) final int numThreads,
                                          final WindowManager windowManager,
                                          @Parameter(OverlappingRatio.class) final double reusingRatio,
                                          final SelectionAlgorithm<T> selectionAlgorithm) {
    this.partialTimespans = partialTimespans;
    this.timescales = tsParser.timescales;
    this.wSizeMap = new HashMap<>();
    this.period = periodCalculator.getPeriod();
    this.startTime = startTime;
    this.finalTimespans = outputLookupTable;
    this.numThreads = numThreads;
    this.reusingRatio = reusingRatio;
    this.wSize = findWsize(windowManager.timescales);
    this.selectionAlgorithm = selectionAlgorithm;
    this.windowManager = windowManager;
    this.pointers = new ArrayList<>(wSize);
    this.positions = new Stack<>();
    this.timespans = new ArrayList<>(wSize);
    this.largestWindowSize = (int)windowManager.timescales.get(windowManager.timescales.size()-1).windowSize;
    // create dependency graph.

    // Initialize
    for (int i = 0; i < wSize; i++) {
      pointers.add(i + 1);
      timespans.add(new Timespan(0, 0, null));
    }
    pointers.set(wSize - 1, 0);
    currInd = 0;
    prevInd = wSize - 1;

    addOverlappingWindowNodeAndEdge();
    System.out.println("end");
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

  private List<Node<T>> findCoalescingPartials(final long time) {
    Node<T> partialNode = partialTimespans.getNextPartialTimespanNode(time);

    if (partialNode.refCnt.get() != 1) {
      return null;
    }

    final List<Node<T>> coalescingPartials = new LinkedList<>();
    final Set<Node<T>> parents = partialNode.parents;
    long t = time;

    do {
      coalescingPartials.add(partialNode);
      t = partialTimespans.getNextSliceTime(t);
      partialNode = partialTimespans.getNextPartialTimespanNode(t);
    } while (partialNode.refCnt.get() == 1 && partialNode.parents.equals(parents));


    return coalescingPartials;
  }

  private void removeUnnecessaryPartialNodes() {
    long time = startTime;

    // find largest partial nodes that can be coalesced.
    while (time < period) {
      final List<Node<T>> coalescingPartials = findCoalescingPartials(time);
      if (coalescingPartials == null || coalescingPartials.size() == 1) {
        // skip
        time = partialTimespans.getNextSliceTime(time);
      } else {
        // coalescing
        final long startTime = coalescingPartials.get(0).start;
        final long endTime = coalescingPartials.get(coalescingPartials.size()-1).end;

        // This just contains one parent
        final Set<Node<T>> parentNode = coalescingPartials.get(0).parents;

        for (final Node<T> removablePartial : coalescingPartials) {
          if (!partialTimespans.removePartialNode(removablePartial.start)) {
            throw new RuntimeException("This partial node is not added in partialTimespans: " + removablePartial);
          } else {
            for (final Node<T> parent : parentNode) {
              parent.getDependencies().remove(removablePartial);
            }
          }
        }

        // add new partial
        if (!partialTimespans.addPartialNode(startTime, endTime)) {
          throw new RuntimeException("Collision while adding [" + startTime + ", " + endTime +") partial node");
        }

        final Node<T> newPartial = partialTimespans.getNextPartialTimespanNode(startTime);
        // update parent child
        for (final Node<T> parent : parentNode) {
          parent.addDependency(newPartial);
        }

        time = endTime;
      }
    }
  }

  private final List<Timescale> getWindows(final long time) {
    final List<Timescale> endWindows = new LinkedList<>();
    for (final Timescale timescale : windowManager.timescales) {
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

  /*
  private void addIntermediateEdge(final Node<T> parent) {
    //System.out.println("child node start: " + parent);
    final List<Node<T>> childNodes = selectionAlgorithm.selection(parent.start, parent.end);
    //System.out.println("child node end: " + parent);
    LOG.log(Level.FINE, "(" + parent.start + ", " + parent.end + ") dependencies1: " + childNodes);
    //System.out.println("(" + parent.start + ", " + parent.end + ") dependencies1: " + childNodes);
    for (final Node<T> elem : childNodes) {
      if (elem.intermediate && elem.getDependencies().size() == 0 &&
          elem.childAdded.compareAndSet(false, true)) {
        addIntermediateEdge(elem);
      }
      parent.addDependency(elem);
    }
  }
  */

  private List<Node<T>> getIntermediateNodes() {
    final List<Node<T>> interNodes = new LinkedList<>();

    for (long tickTime = startTime + 1; tickTime <= startTime + period; tickTime++) {
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
              interNodes.add(newNode);
            }
          }
        }

        final int uu = positions.pop();
      }

      prevInd = currInd;
      currInd += 1;
      if (currInd == wSize) {
        currInd = 0;
      }
    }

    return interNodes;
  }
  /**
   * Add DependencyGraphNode and connect dependent nodes.
   */
  private void addOverlappingWindowNodeAndEdge() {

    final List<List<Node<T>>> nodes = new ArrayList<List<Node<T>>>((int)period+1);

    for (int i = 0; i <= period; i++) {
      nodes.add(null);
    }

    for (long endTime = 1; endTime <= period; endTime += 1) {
      final List<Node<T>> nodesAtTimeT = new LinkedList<>();
      for (final Timescale ts : timescales) {
        if (endTime % ts.intervalSize == 0) {
          final long startTime = endTime - ts.windowSize;
          final Node node = new Node(startTime, endTime, false, ts);
          node.isNotShared = false;
          node.intermediate = false;
          nodesAtTimeT.add(node);

          finalTimespans.saveOutput(startTime, endTime, node);
        }
      }

      nodes.set((int)endTime, nodesAtTimeT);
    }

    // Add intermediate nodes
    final List<Node<T>> interNodes = getIntermediateNodes();
    interNodes.parallelStream().forEach(interNode -> {
      final List<Node<T>> nodesAtTimeT = nodes.get((int)interNode.end);
      synchronized (nodesAtTimeT) {
        int index = 0;
        for (final Node<T> nodeAtTimeT : nodesAtTimeT) {
          if (nodeAtTimeT.start > interNode.start) {
            index += 1;
          } else if (nodeAtTimeT.start < interNode.start) {
            break;
          } else {
            // same. we dont have to add internode
            index = -1;
          }
        }

        if (index >= 0) {
          nodesAtTimeT.add(index, interNode);
          finalTimespans.saveOutput(interNode.start, interNode.end, interNode);
        }
      }
    });

    nodes.stream().forEach(nodesAtTimeT -> {
      if (nodesAtTimeT != null) {
        for (final Node<T> node : nodesAtTimeT) {
          addEdge(node);
        }
      }
    });

    // Remove unnecessary intermediate nodes
    final AtomicBoolean isChanged = new AtomicBoolean(false);
    do {
      isChanged.set(false);
      interNodes.parallelStream()
          .filter(node -> node.refCnt.get() == 0 && node.getDependencies().size() > 0)
          .forEach(node -> {
            finalTimespans.deleteOutput(node.start, node.end);
            for (final Node<T> child : node.getDependencies()) {
              child.decreaseRefCntWithoutReset();
              synchronized (child.parents) {
                child.parents.remove(node);
              }
            }
            node.getDependencies().clear();
            isChanged.set(true);
          });
    } while (isChanged.get());


    // Build intermediate nodes
    //adjustPartialNodes();

    // Remove partial nodes that have reference count 1
    removeUnnecessaryPartialNodes();
  }

  private void adjustPartialNodes() {
    for (long start = startTime; start < period; start = partialTimespans.getNextSliceTime(start)) {
      adjustNodes(start);
    }
  }

  private void adjustNodes(final long startTime) {
    final Node<T> initialNode = partialTimespans.getNextPartialTimespanNode(startTime);
    final Set<Node<T>> currentParents = new HashSet<>(initialNode.parents);
    final List<Node<T>> currentChild = new LinkedList<>();
    currentChild.add(initialNode);

    for (long start = partialTimespans.getNextSliceTime(startTime);
         start < period && currentParents.size() > 0; start = partialTimespans.getNextSliceTime(start)) {
      final Node<T> partial = partialTimespans.getNextPartialTimespanNode(start);
      final Set<Node<T>> removedParents = new HashSet<>(currentParents);
      removedParents.removeAll(partial.parents);

      currentParents.removeAll(removedParents);

      if (removedParents.isEmpty()) {
        currentChild.add(partial);
      } else {
        if (currentChild.size() > 1) {
          final Node<T> newIntermediate =
              new Node<T>(currentChild.get(0).start, currentChild.get(currentChild.size()-1).end, 1);

          try {
            // If the node already exists, stop
            finalTimespans.lookup(newIntermediate.start, newIntermediate.end);
            break;
          } catch (final NotFoundException e) {
            finalTimespans.saveOutput(newIntermediate.start, newIntermediate.end, newIntermediate);
          }

          // new intermedate node
          // child ....
          // Add dependency
          for (final Node<T> child : currentChild) {
            newIntermediate.addDependency(child);
          }

          // Adjust dependency
          for (final Node<T> parent : removedParents) {
            final List<Node<T>> parentChild = parent.getDependencies();
            // Remove prev child
            for (final Node<T> child : currentChild) {
              parentChild.remove(child);
              child.refCnt.decrementAndGet();
              child.initialRefCnt.decrementAndGet();
              child.parents.remove(parent);
            }

            // Re-connect the new intermediate node
            parent.addDependency(newIntermediate);
          }

          for (final Node<T> parent : currentParents) {
            final List<Node<T>> parentChild = parent.getDependencies();
            // Remove prev child
            for (final Node<T> child : currentChild) {
              parentChild.remove(child);
              child.refCnt.decrementAndGet();
              child.initialRefCnt.decrementAndGet();
              child.parents.remove(parent);
            }

            // Re-connect the new intermediate node
            parent.addDependency(newIntermediate);
          }

          // Remove current child
          currentChild.clear();
          currentChild.add(newIntermediate);

          //System.out.println("Intermediate: " + newIntermediate + ", parent: " + newIntermediate.parents + ", child: " +newIntermediate.getDependencies());
        }

        currentChild.add(partial);
      }
    }
  }

  private void addEdge(final Node<T> parent) {
    //System.out.println("child node start: " + parent);
    final List<Node<T>> childNodes = selectionAlgorithm.selection(parent.start, parent.end);
    //System.out.println("child node end: " + parent);
    LOG.log(Level.FINE, "(" + parent.start + ", " + parent.end + ") dependencies1: " + childNodes);
    //System.out.println("(" + parent.start + ", " + parent.end + ") dependencies1: " + childNodes);

    // Find dependent node
    for (final Node<T> elem : childNodes) {
      parent.addDependency(elem);
      if (elem.end == parent.end) {
        parent.height = elem.height + 1;
      }
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

  final class Info {
    public final List<Node<T>> nodes;
    public final Timescale timescale;

    public Info(final List<Node<T>> nodes,
                final Timescale timescale) {
      this.nodes = nodes;
      this.timescale = timescale;
    }
  }
}