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
public final class PruningParallelMaxDependencyGraphImpl<T> implements DependencyGraph {

  private static final Logger LOG = Logger.getLogger(PruningParallelMaxDependencyGraphImpl.class.getName());

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
  private PruningParallelMaxDependencyGraphImpl(final TimescaleParser tsParser,
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

    final Set<Node<T>> startSet = new HashSet<>();
    final Set<Node<T>> endSet = new HashSet<>();

    for (int index = startIndex; index < infos.size(); index++) {
      // largest windows
      final Info info = infos.get(index);
      final long windowSize = info.timescale.windowSize;

      if (windowSize < (node.end - node.start)) {
        throw new RuntimeException("Invalid window: " + info.timescale + ", node: " + node);
      }

      startSet.addAll(info.startTimeTree.subSet(new Node<T>(node.end - windowSize, 1, false), node));
      endSet.addAll(info.endTimeTree.subSet(node, new Node<T>(1, node.start + windowSize, false)));

      if (node.end - windowSize < 0) {
        // Add minus nodes
        startSet.addAll(info.startTimeTree.subSet(new Node<T>(node.end - windowSize + period, 1, false),
            new Node<T>(node.start + period, 1, false)));
        endSet.addAll(info.endTimeTree.subSet(new Node<T>(1, node.end + period, false),
            new Node<T>(1, node.start + windowSize + period, false)));
      }
    }

    startSet.retainAll(endSet);

    return startSet;
  }

  private Set<Node<T>> getIncludedNode(final Node<T> node,
                                       final List<Info> infos) {

    final int startIndex = getIndexOfInfos(node, infos) - 1;
    if (startIndex < 0) {
      return new HashSet<>();
    }

    final Set<Node<T>> nodesInStart = new HashSet<>();
    final Set<Node<T>> nodesInEnd = new HashSet<>();

    final Set<Node<T>> added = new HashSet<>();

    for (int index = startIndex; index >= 0; index--) {
      final Info info = infos.get(index);
      final long windowSize = info.timescale.windowSize;

      final Node<T> startIndexNode = new Node<>(node.end - windowSize, 1, false);
      final Node<T> endIndexNode = new Node<>(1, node.start + windowSize, false);

      nodesInStart.addAll(info.startTimeTree.subSet(node, startIndexNode));
      nodesInEnd.addAll(info.endTimeTree.subSet(endIndexNode, node));

      // minus nodes
      if (node.start < 0) {
        final Node<T> startIndexNode1 = new Node<>(node.start + period, node.start + period + 1, false);
        added.addAll(info.startTimeTree.tailSet(startIndexNode1));
      }
    }


    nodesInStart.retainAll(nodesInEnd);
    nodesInStart.addAll(added);

    return nodesInStart;
  }

  private void pruning(final List<Info> infos, final List<Node<T>> addedNodes) {


    final int selectNum = reusingRatio > 0.0 ? (int)(addedNodes.size() * reusingRatio) : sharedFinalNum;

    if (selectNum >= addedNodes.size()) {
      return;
    }

    final boolean add =  true;//selectNum <=  addedNodes.size()/2 ? true : false;


    // Calculate possible counts
    addedNodes.parallelStream().forEach(node -> {
      //parentSetMap.put(node, getPossibleParents(node, startTimeTree, endTimeTree));

      //node.possibleParentCount = parentSetMap.get(node).size();
      node.possibleParentCount = getPossibleParents(node, infos).size();

      node.cost = node.possibleParentCount * (node.end - node.start);
      node.isNotShared = add;
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

      maxNode.isNotShared = !add;

      // Select included nodes
      s1 = System.currentTimeMillis();
      final Set<Node<T>> includedNodes = getIncludedNode(maxNode, infos);
      System.out.println("includedNodes time: " + (System.currentTimeMillis() - s1));

      s1 = System.currentTimeMillis();

      includedNodes.parallelStream()
          .filter(includedNode -> includedNode.isNotShared == add)
          .forEach(includedNode -> {
            final Set<Node<T>> includedNodeParent;
            if (possibleParentMap.get(includedNode) == null) {
              includedNodeParent =  getPossibleParents(includedNode, infos);
            } else {
              includedNodeParent = possibleParentMap.get(includedNode);
            }

            includedNodeParent.removeAll(nParentSet);
            includedNode.possibleParentCount = includedNodeParent.size();
            includedNode.cost = includedNode.possibleParentCount * (includedNode.end - includedNode.start);

            if (includedNode.possibleParentCount > 500) {
              possibleParentMap.put(includedNode, includedNodeParent);
            } else {
              possibleParentMap.remove(includedNode);
            }
          });

      System.out.println("Future time: " + (System.currentTimeMillis() - s1));

      maxNode.possibleParentCount = 0;
      maxNode.cost = add ? 0 : Long.MAX_VALUE;
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

        final TreeSet<Node<T>> startTimeTree = new TreeSet<Node<T>>(new Comparator<Node<T>>() {
          @Override
          public int compare(final Node<T> o1, final Node<T> o2) {
            return o1.start < o2.start ? -1 : 1;
          }
        });

        final TreeSet<Node<T>> endTimeTree = new TreeSet<Node<T>>(new Comparator<Node<T>>() {
          @Override
          public int compare(final Node<T> o1, final Node<T> o2) {
            return o1.end < o2.end ? -1 : 1;
          }
        });

        final List<Node<T>> nodes = new ArrayList<Node<T>>((int)(period / timescale.intervalSize + 1));

        for (long time = timescale.intervalSize + startTime; time <= period + startTime; time += timescale.intervalSize) {

          // create vertex and add it to the table cell of (time, windowsize)
          final long start = time - timescale.windowSize;
          final Node parent = new Node(start, time, false, timescale);
          parent.isNotShared = false;

          startTimeTree.add(parent);
          endTimeTree.add(parent);

          nodes.add(parent);

          //table[(int)start+largestWindowSize][(int)time+largestWindowSize] = true;
          //nodeTable[(int)start+largestWindowSize][(int)time+largestWindowSize] = parent;

          finalTimespans.saveOutput(start, time, parent);
          LOG.log(Level.FINE, "Saved to table : (" + start + " , " + time + ") refCount : " + parent.refCnt);
          //LOG.log(Level.FINE, "(" + start + ", " + time + ") dependencies2: " + childNodes);
        }

        return new Info(nodes, timescale, startTimeTree, endTimeTree);
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
    public final TreeSet<Node<T>> startTimeTree;
    public final TreeSet<Node<T>> endTimeTree;

    public Info(final List<Node<T>> nodes,
                final Timescale timescale,
                final TreeSet<Node<T>> startTimeTree,
                final TreeSet<Node<T>> endTimeTree) {
      this.nodes = nodes;
      this.timescale = timescale;
      this.startTimeTree = startTimeTree;
      this.endTimeTree = endTimeTree;
    }
  }
}
