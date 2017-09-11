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
import vldb.operator.window.timescale.parameter.*;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
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


  private final WindowManager windowManager;
  private final int largestWindowSize;

  /**
   * DependencyGraph constructor. This first builds the dependency graph.
   * @param startTime the initial start time of when the graph is built.
   */
  @Inject
  private PruningDependencyGraphImpl(final TimescaleParser tsParser,
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

  private Set<Node<T>> getPossibleParents(final Node<T> node,
                                 final TreeSet<Node<T>> startTimeTree,
                                 final TreeSet<Node<T>> endTimeTree) {

    final Set<Node<T>> startSet = startTimeTree.subSet(new Node<T>(Math.min(node.start-1, node.end - largestWindowSize), node.end, false), node);
    final Set<Node<T>> endSet = endTimeTree.subSet(node, new Node<T>(node.start, Math.max(node.end+1, node.start + largestWindowSize), false));

    final Set<Node<T>> intersect = new HashSet<>(startSet);
    final Set<Node<T>> intersect1 = new HashSet<>(endSet);
    intersect.retainAll(intersect1);
    intersect.remove(node);

    // Minus nodes
    final Set<Node<T>> startSet1 = startTimeTree.subSet(new Node<T>(node.start - period - largestWindowSize, node.start, false),
        new Node<T>(node.start - period, node.start, false));
    final Set<Node<T>> endSet1 = endTimeTree.subSet(new Node<T>(1, node.end - period, false), new Node<T>(1, node.end - period + largestWindowSize, false));

    final Set<Node<T>> intersect2 = new HashSet<>(startSet1);
    final Set<Node<T>> intersect3 = new HashSet<>(endSet1);
    intersect2.retainAll(intersect3);
    intersect2.remove(node);

    intersect.addAll(intersect2);
    return intersect;
  }

  private Set<Node<T>> getIncludedNode(final Node<T> node,
                                        final TreeSet<Node<T>> startTimeTree,
                                        final TreeSet<Node<T>> endTimeTree) {

    final Node<T> startIndexNode = new Node<>(node.end-1, node.end, false);
    final Node<T> endIndexNode = new Node<>(node.start, node.start+1, false);

    final Set<Node<T>> nodesInStart = startTimeTree.subSet(node, startIndexNode);
    final Set<Node<T>> nodesInEnd = endTimeTree.subSet(endIndexNode, node);

    final Set<Node<T>> includedNodes = new HashSet<>(nodesInStart);
    includedNodes.addAll(nodesInEnd);

    includedNodes.remove(node);

    if (node.start < 0) {
      final Node<T> startIndexNode1 = new Node<>(node.start + period, node.start + period + 1, false);

      final Set<Node<T>> nodes = startTimeTree.tailSet(startIndexNode1);
      includedNodes.addAll(nodes);
    }

    return includedNodes;
  }

  private void pruning(final List<Node<T>> addedNodes) {
    final PriorityBlockingQueue<Node<T>> priorityQueue;
    final ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
    final int selectNum = (int)(addedNodes.size() * reusingRatio);
    final boolean add = (addedNodes.size() - selectNum) >=  addedNodes.size()/2 ? true : false;
    if (add) {
      priorityQueue = new PriorityBlockingQueue<>(addedNodes.size(), new Comparator<Node<T>>() {
        @Override
        public int compare(final Node<T> o1, final Node<T> o2) {
          if (o1.cost < o2.cost) {
            return 1;
          } else if (o1.cost > o2.cost) {
            return -1;
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
      });
    } else {
      priorityQueue = new PriorityBlockingQueue<>(addedNodes.size(), new Comparator<Node<T>>() {
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
      });
    }

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

    //final ConcurrentMap<Node<T>, Set<Node<T>>> parentSetMap = new ConcurrentHashMap<>(addedNodes.size());

    // Counting the possible reference count!
    for (final Node<T> node : addedNodes) {
      startTimeTree.add(node);
      endTimeTree.add(node);
    }

    for (final Node<T> node : addedNodes) {
      executorService.submit(() -> {
        //parentSetMap.put(node, getPossibleParents(node, startTimeTree, endTimeTree));

        //node.possibleParentCount = parentSetMap.get(node).size();
        node.possibleParentCount = getPossibleParents(node, startTimeTree, endTimeTree).size();

        node.cost = node.possibleParentCount * (node.end - node.start);
        node.isNotShared = add;
        priorityQueue.add(node);
      });
    }

    executorService.shutdown();
    try {
      executorService.awaitTermination(1000, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    // Pruning start!

    int numSelection = add ? selectNum : addedNodes.size() - selectNum;
    final AtomicInteger currentNum = new AtomicInteger(0);
    final ExecutorService executorService2 = Executors.newFixedThreadPool(numThreads);

    final Map<Node<T>, Set<Node<T>>> possibleParentMap = new ConcurrentHashMap<>();

    while (currentNum.get() < numSelection) {
      System.out.println(currentNum.get() + " / " + numSelection);
      final Node<T> n = priorityQueue.poll();
      //final Set<Node<T>> nParentSet = parentSetMap.remove(n);
      final Set<Node<T>> nParentSet = getPossibleParents(n, startTimeTree, endTimeTree);
      possibleParentMap.remove(n);
      n.isNotShared = !add;

      // Select included nodes
      final Set<Node<T>> includedNodes = getIncludedNode(n, startTimeTree, endTimeTree);
      final List<Future> futures = new ArrayList<>(includedNodes.size());

      for (final Node<T> includedNode : includedNodes) {
        if (includedNode.isNotShared == add) {
          futures.add(executorService2.submit(() -> {
              //final Set<Node<T>> includedNodeParent = parentSetMap.get(includedNode);
            if (possibleParentMap.get(includedNode) == null) {
              possibleParentMap.put(includedNode, getPossibleParents(includedNode, startTimeTree, endTimeTree));
            }
            final Set<Node<T>> includedNodeParent = possibleParentMap.get(includedNode);

            includedNodeParent.removeAll(nParentSet);
            includedNode.possibleParentCount = includedNodeParent.size();
            includedNode.cost = includedNode.possibleParentCount * (includedNode.end - includedNode.start);

            priorityQueue.remove(includedNode);
            priorityQueue.add(includedNode);
           }));
        }
      }

      for (final Future future : futures) {
        try {
          future.get();
        } catch (InterruptedException e) {
          e.printStackTrace();
        } catch (ExecutionException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      }
      currentNum.incrementAndGet();
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


  /**
   * Add DependencyGraphNode and connect dependent nodes.
   */
  private void addOverlappingWindowNodeAndEdge() {
    final ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
    final List<Node<T>> addedNodes = new LinkedList<>();

    //System.out.println("period: " + period);
    /*
    final boolean[][] table = new boolean[(int)period + largestWindowSize][(int)period+largestWindowSize+1];
    final Node[][] nodeTable = new Node[(int)period + largestWindowSize][(int)period+largestWindowSize+1];

    for (int i = 0; i < period + largestWindowSize; i++) {
      for (int j = 0; j < period  + largestWindowSize + 1; j++) {
        table[i][j] = false;
        nodeTable[i][j] = null;
      }
    }
    */

    if (numThreads == 1) {
      for (final Timescale timescale : timescales) {

        for (long time = timescale.intervalSize + startTime; time <= period + startTime; time += timescale.intervalSize) {
          // create vertex and add it to the table cell of (time, windowsize)
          final long start = time - timescale.windowSize;
          final Node parent = new Node(start, time, false);
          addedNodes.add(parent);


          //table[(int)start+largestWindowSize][(int)time+largestWindowSize] = true;
          //nodeTable[(int)start+largestWindowSize][(int)time+largestWindowSize] = parent;

          finalTimespans.saveOutput(start, time, parent);
          LOG.log(Level.FINE, "Saved to table : (" + start + " , " + time + ") refCount : " + parent.refCnt);
          //LOG.log(Level.FINE, "(" + start + ", " + time + ") dependencies2: " + childNodes);
        }
      }
    } else {
      final List<Future<List<Node<T>>>> futures = new LinkedList<>();
      for (final Timescale timescale : timescales) {
        futures.add(executorService.submit(new Callable<List<Node<T>>>() {
          @Override
          public List<Node<T>> call() throws Exception {
            final List<Node<T>> createdNodes = new LinkedList<Node<T>>();
            for (long time = timescale.intervalSize + startTime; time <= period + startTime; time += timescale.intervalSize) {

              // create vertex and add it to the table cell of (time, windowsize)
              final long start = time - timescale.windowSize;
              final Node parent = new Node(start, time, false);
              createdNodes.add(parent);

              //table[(int)start+largestWindowSize][(int)time+largestWindowSize] = true;
              //nodeTable[(int)start+largestWindowSize][(int)time+largestWindowSize] = parent;

              finalTimespans.saveOutput(start, time, parent);
              LOG.log(Level.FINE, "Saved to table : (" + start + " , " + time + ") refCount : " + parent.refCnt);
              //LOG.log(Level.FINE, "(" + start + ", " + time + ") dependencies2: " + childNodes);
            }
            return createdNodes;
          }
        }));
      }

      for (final Future<List<Node<T>>> future : futures) {
        try {
          addedNodes.addAll(future.get());
        } catch (InterruptedException e) {
          e.printStackTrace();
        } catch (ExecutionException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
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
