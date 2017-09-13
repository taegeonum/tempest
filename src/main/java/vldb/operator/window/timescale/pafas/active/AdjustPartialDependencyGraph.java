
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
import vldb.operator.window.timescale.pafas.dynamic.WindowManager;
import vldb.operator.window.timescale.parameter.NumThreads;
import vldb.operator.window.timescale.parameter.OverlappingRatio;
import vldb.operator.window.timescale.parameter.SharedFinalNum;
import vldb.operator.window.timescale.parameter.StartTime;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * DependencyGraph shows which nodes are related to each other. This is used so that unnecessary outputs are not saved.
 */
public final class AdjustPartialDependencyGraph<T> implements DependencyGraph {

  private static final Logger LOG = Logger.getLogger(AdjustPartialDependencyGraph.class.getName());

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

  private final int sharedFinalNum;
  /**
   * DependencyGraph constructor. This first builds the dependency graph.
   * @param startTime the initial start time of when the graph is built.
   */
  @Inject
  private AdjustPartialDependencyGraph(final TimescaleParser tsParser,
                                       @Parameter(StartTime.class) final long startTime,
                                       final PartialTimespans partialTimespans,
                                       final PeriodCalculator periodCalculator,
                                       @Parameter(SharedFinalNum.class) final int sharedFinalNum,
                                       final OutputLookupTable<Node<T>> outputLookupTable,
                                       @Parameter(NumThreads.class) final int numThreads,
                                       final WindowManager windowManager,
                                       @Parameter(OverlappingRatio.class) final double reusingRatio,
                                       final SelectionAlgorithm<T> selectionAlgorithm) {
    this.partialTimespans = partialTimespans;
    this.timescales = tsParser.timescales;
    this.period = periodCalculator.getPeriod();
    this.startTime = startTime;
    this.sharedFinalNum = sharedFinalNum;
    this.finalTimespans = outputLookupTable;
    this.numThreads = numThreads;
    this.reusingRatio = reusingRatio;
    this.selectionAlgorithm = selectionAlgorithm;
    this.windowManager = windowManager;
    this.largestWindowSize = (int)windowManager.timescales.get(windowManager.timescales.size()-1).windowSize;
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

  private int getIndexOfInfos(final Node node, final List<Info> infos) {
    for (int i = 0; i < infos.size(); i++) {
      if (node.timescale == infos.get(i).timescale) {
        return i;
      }
    }

    throw new RuntimeException("No matched timescale: " + node.timescale + ", infos: " + infos);
  }

  private Set<Node<T>> getPossibleParents(final Node<T> node,
                                          final List<Info> infos) {
    final int startIndex = getIndexOfInfos(node, infos) + 1;

    if (startIndex == infos.size()) {
      return new HashSet<>();
    }

    final Set<Node<T>> parentSet = new HashSet<>();

    for (int index = startIndex; index < infos.size(); index++) {
      // largest windows
      final Info info = infos.get(index);
      final long windowSize = info.timescale.windowSize;
      final long interval = info.timescale.intervalSize;

      if (windowSize < (node.end - node.start)) {
        throw new RuntimeException("Invalid window: " + info.timescale + ", node: " + node);
      }

      final long endStart = interval * ((int)Math.ceil(((double)node.end) / interval));

      for (long e = endStart; e - windowSize <= node.start; e += interval) {
        final long s = e - windowSize;
        if (e > period) {
          try {
            parentSet.add(finalTimespans.lookup(s - period, e - period));
          } catch (NotFoundException e1) {
            e1.printStackTrace();
            throw new RuntimeException(e1);
          }
        } else {
          try {
            parentSet.add(finalTimespans.lookup(s, e));
          } catch (NotFoundException e1) {
            e1.printStackTrace();
            throw new RuntimeException(e1);
          }
        }
      }
    }

    return parentSet;
  }

  private Collection<Node<T>> getIncludedNode(final Node<T> node,
                                              final List<Info> infos) {

    final Set<Node<T>> added = new HashSet<>();
    final int startIndex = getIndexOfInfos(node, infos);
    for (int index = startIndex; index >= 0; index--) {
      final Info info = infos.get(index);

      final long windowSize = info.timescale.windowSize;
      final long interval = info.timescale.intervalSize;

      long endStart = node.start % interval == 0 ? node.start + interval : interval * ((int)Math.ceil(((double)node.start) / interval));

      for (long e = endStart; e - windowSize < node.end; e += interval) {
        final long s = e - windowSize;
        if ((s > node.start && s < node.end) || (e > node.start &&  e < node.end)) {
          try {
            if (e <= 0) {
              added.add(finalTimespans.lookup(s + period, e + period));
            } else if (e > period) {
              added.add(finalTimespans.lookup(s - period, e - period));
            } else {
              added.add(finalTimespans.lookup(s, e));
            }
          } catch (NotFoundException e1) {
            e1.printStackTrace();
            throw new RuntimeException("window: " + info.timescale + ", s: " + s + ", e: " + e + ", node: " + node + ", " + e1);
          }
        }
      }
    }

    return added;
  }

  private void buildIntermediateNodes(final long startTime) {
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

  private void adjustPartialNodes() {
    for (long start = startTime; start < period; start = partialTimespans.getNextSliceTime(start)) {
      buildIntermediateNodes(start);
    }
  }

  private void pruning(final List<Info> infos, final List<Node<T>> addedNodes) {


    final int selectNum = reusingRatio > 0.0 ? (int)(addedNodes.size() * reusingRatio) : sharedFinalNum;

    if (selectNum >= addedNodes.size()) {
      return;
    }

    // Calculate possible counts
    addedNodes.parallelStream().forEach(node -> {
      //parentSetMap.put(node, getPossibleParents(node, startTimeTree, endTimeTree));

      //node.possibleParentCount = parentSetMap.get(node).size();
      node.possibleParentCount = getPossibleParents(node, infos).size();

      node.cost = node.possibleParentCount * (node.end - node.start);
      node.isNotShared = true;
    });

    // Pruning start!
    final Comparator<Node<T>> addComparator = new Comparator<Node<T>>() {
      @Override
      public int compare(final Node<T> o1, final Node<T> o2) {
        if (o1.cost < o2.cost) {
          return -1;
        } else if (o1.cost > o2.cost) {
          return 1;
        } else {
          final long o1Size = o1.end - o1.start;
          final long o2Size = o2.end - o2.start;
          if (o1Size < o2Size) {
            return 1;
          } else if (o1Size > o2Size) {
            return -1;
          } else {
            return 0;
          }
        }
      }
    };

    final List<Node<T>> filteredNodes = addedNodes
        .parallelStream()
        .filter(node -> node.cost > 0)
        .collect(Collectors.toCollection(ArrayList::new));

    int numSelection = selectNum;//add ? selectNum : filteredNodes.size() - selectNum;
    int currentNum = 0;

    final Map<Node<T>, Set<Node<T>>> possibleParentMap = new ConcurrentHashMap<>();
    final Comparator<Node<T>> comparator = addComparator;

    while (currentNum < numSelection) {

      long s1 = System.currentTimeMillis();
      final Node<T> maxNode = filteredNodes.parallelStream().max(comparator).get();
      System.out.println(currentNum + " / " + numSelection + ", maxnode: " + maxNode);
      System.out.println("max time: " + (System.currentTimeMillis() - s1));

      s1 = System.currentTimeMillis();
      final Set<Node<T>> nParentSet = possibleParentMap.get(maxNode) == null
          ? getPossibleParents(maxNode, infos) : possibleParentMap.remove(maxNode);

      System.out.println("nParentSet time: " + (System.currentTimeMillis() - s1));

      maxNode.isNotShared = false;

      // Select included nodes
      s1 = System.currentTimeMillis();
      final Collection<Node<T>> includedNodes = getIncludedNode(maxNode, infos);
      System.out.println("includedNodes time: " + (System.currentTimeMillis() - s1));

      s1 = System.currentTimeMillis();

      includedNodes.parallelStream()
          .filter(includedNode -> includedNode.isNotShared == true)
          .forEach(includedNode -> {
            final Set<Node<T>> includedNodeParent;
            if (possibleParentMap.get(includedNode) == null) {
              includedNodeParent =  getPossibleParents(includedNode, infos);
              possibleParentMap.put(includedNode, includedNodeParent);
            } else {
              includedNodeParent = possibleParentMap.get(includedNode);
            }

            includedNodeParent.removeAll(nParentSet);
            includedNode.possibleParentCount = includedNodeParent.size();
            includedNode.cost = includedNode.possibleParentCount * (includedNode.end - includedNode.start);

          });

      System.out.println("Future time: " + (System.currentTimeMillis() - s1));

      maxNode.possibleParentCount = 0;
      maxNode.cost = 0;
      currentNum += 1;
    }

    // Sorting by possible parent count
    /*
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
    final int pruningNum = array.length - sharedFinalNum;
    for (int i = 0; i < pruningNum; i++) {
      array[i].isNotShared = true;
    }
    */

    /*
    final int pruningIndex = Math.min(array.length-1, (int)(reusingRatio * array.length));
    for (int i = pruningIndex; i < array.length; i++) {
      array[i].isNotShared = true;
      //System.out.println("Not shared node: " + array[i] + ", cnt: " + array[i].possibleParentCount);
    }
    */
  }

  private int calculateSize() {
    int s = 0;
    for (final Timescale timescle : timescales) {
      s += (period / timescle.intervalSize);
    }
    return s;
  }

  /**
   * Add DependencyGraphNode and connect dependent nodes.
   */
  private void addOverlappingWindowNodeAndEdge() {
    final List<Info> infos = timescales.parallelStream().map(timescale -> {

      final List<Node<T>> nodes = new ArrayList<Node<T>>((int)(period / timescale.intervalSize + 1));

      for (long time = timescale.intervalSize + startTime; time <= period + startTime; time += timescale.intervalSize) {

        // create vertex and add it to the table cell of (time, windowsize)
        final long start = time - timescale.windowSize;
        final Node parent = new Node(start, time, false, timescale);
        parent.isNotShared = false;

        nodes.add(parent);

        //table[(int)start+largestWindowSize][(int)time+largestWindowSize] = true;
        //nodeTable[(int)start+largestWindowSize][(int)time+largestWindowSize] = parent;

        finalTimespans.saveOutput(start, time, parent);
        LOG.log(Level.FINE, "Saved to table : (" + start + " , " + time + ") refCount : " + parent.refCnt);
        //LOG.log(Level.FINE, "(" + start + ", " + time + ") dependencies2: " + childNodes);
      }

      return new Info(nodes, timescale);
    }).collect(Collectors.toCollection(ArrayList::new));

    infos.sort(new Comparator<Info>() {
      @Override
      public int compare(final Info o1, final Info o2) {
        if (o1.timescale.windowSize - o2.timescale.windowSize < 0) {
          return -1;
        } else {
          return 1;
        }
      }
    });

    final List<Node<T>> addedNodes = infos.stream().flatMap(info -> info.nodes.stream())
        .collect(Collectors.toCollection(ArrayList::new));

    pruning(infos, addedNodes);

    addedNodes.parallelStream().forEach(node -> addEdge(node));

    // Adjust partial nodes!
    adjustPartialNodes();
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