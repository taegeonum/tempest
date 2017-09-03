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
import vldb.operator.window.timescale.pafas.PeriodCalculator;
import vldb.operator.window.timescale.pafas.dynamic.WindowManager;
import vldb.operator.window.timescale.parameter.NumThreads;
import vldb.operator.window.timescale.parameter.ReusingRatio;
import vldb.operator.window.timescale.parameter.StartTime;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * DependencyGraph shows which nodes are related to each other. This is used so that unnecessary outputs are not saved.
 */
public final class FineGrainedPruningRebuildDependencyGraphImpl<T> implements DependencyGraph {

  private static final Logger LOG = Logger.getLogger(FineGrainedPruningRebuildDependencyGraphImpl.class.getName());

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

  private final ActivePartialTimespans<T> partialTimespans;
  private final OutputLookupTable<Node<T>> finalTimespans;
  private final SelectionAlgorithm<T> selectionAlgorithm;
  private final int numThreads;

  private final Set<Node<T>> noSharedNode = new HashSet<>();

  private final ConcurrentMap<Long, List<Long>> startEndMap;

  private final WindowManager windowManager;
  private final int largestWindowSize;

  /**
   * DependencyGraph constructor. This first builds the dependency graph.
   * @param startTime the initial start time of when the graph is built.
   */
  @Inject
  private FineGrainedPruningRebuildDependencyGraphImpl(final TimescaleParser tsParser,
                                                       @Parameter(StartTime.class) final long startTime,
                                                       final ActivePartialTimespans partialTimespans,
                                                       final PeriodCalculator periodCalculator,
                                                       final OutputLookupTable<Node<T>> outputLookupTable,
                                                       @Parameter(NumThreads.class) final int numThreads,
                                                       final WindowManager windowManager,
                                                       @Parameter(ReusingRatio.class) final double reuseRatio,
                                                       final SelectionAlgorithm<T> selectionAlgorithm) {
    this.partialTimespans = partialTimespans;
    this.timescales = tsParser.timescales;
    this.period = periodCalculator.getPeriod();
    this.startTime = startTime;
    this.finalTimespans = outputLookupTable;
    this.numThreads = numThreads;
    this.startEndMap = new ConcurrentHashMap<>();
    this.selectionAlgorithm = selectionAlgorithm;
    this.windowManager = windowManager;
    this.largestWindowSize = (int)windowManager.timescales.get(windowManager.timescales.size()-1).windowSize;
    // create dependency graph.
    addOverlappingWindowNodeAndEdge(reuseRatio);
    LOG.info("done");
  }

  private int calculateWeight(final Node<T> node) {
    final int size = node.getDependencies().size();
    if (node.refCnt.get() == 0) {
      return 0;
    } else if (node.getDependencies().get(size-1).end < node.end) {
      return size + 1;
    } else {
     return node.refCnt.get() * size;
    }
  }

  private boolean isOverlappingRange(final int i, final int j, final int st, final int e, final boolean[][] table) {
    for (int k = i; k <= st; k++) {
      for (int l = j; l >= e; l--) {
        if ((k != i && l != j) && (k != st && l != e)) {
          if (table[k][l]) {
            return true;
          }
        }
      }
    }
    return false;
  }

  private void adjustDependencyGraph2(final double reuseRatio, final List<Node<T>> addedNodes, final boolean[][] table) {
    final ExecutorService executorService = Executors.newFixedThreadPool(numThreads);

    // Counting the possible reference count!
    for (final Node<T> node : addedNodes) {
      executorService.submit(() -> {
        int st = (int) node.start + largestWindowSize;
        int e = (int) node.end + largestWindowSize;

        int cnt = 0;
        for (int i = st; i >= 0; i--) {
          for (int j = e; j <= (int) period + largestWindowSize; j++) {
            if (i != st && j != e) {
              if (table[i][j]) {
                if (!isOverlappingRange(i, j, st, e, table)) {
                  cnt += 1;
                }
              }
            }
          }
        }

        node.possibleParentCount = cnt;
        //System.out.println("Node cnt: " + node + " , " + cnt);
      });
    }

    final Node<T>[] array = new Node[addedNodes.size()];
    for (int i = 0; i < array.length; i++) {
      array[i] = addedNodes.get(i);
    }

    executorService.shutdown();
    try {
      executorService.awaitTermination(300, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    // Sorting by possible parent count
    Arrays.parallelSort(array, new Comparator<Node<T>>() {
      @Override
      public int compare(final Node<T> o1, final Node<T> o2) {
        if (o1.possibleParentCount < o2.possibleParentCount) {
          return 1;
        } else if (o1.possibleParentCount > o2.possibleParentCount) {
          return -1;
        } else {
          return 0;
        }
      }
    });

    int sharedNum = 0;
    // Set is not shared true when the reference count is zero
    for (final Node<T> node : addedNodes) {
      if (node.refCnt.get() == 0) {
        node.isNotShared = true;
      } else {
        sharedNum += 1;
      }
      node.reset();
    }

    partialTimespans.reset();;

    final int pruningNum = (int)((1 - reuseRatio) * sharedNum);
    int deletedNum = 0;
    int index = 0;
    final int size = array.length;
    while (deletedNum < pruningNum && index < size) {
      final Node<T> removableNode = array[index];
      if (!removableNode.isNotShared) {
        removableNode.isNotShared = true;
        deletedNum += 1;
      }
      index += 1;
    }

    final ExecutorService executorService2 = Executors.newFixedThreadPool(numThreads);
    // Rebuild the edges!!
    for (final Node<T> node : addedNodes) {
      executorService2.submit(new Runnable() {
        @Override
        public void run() {
          addEdge(node);
        }
      });
    }

    executorService2.shutdown();
    try {
      executorService2.awaitTermination(1000, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  private void adjustDependencyGraph(final double reuseRatio, final List<Node<T>> addedNodes) {
    final ExecutorService executorService = Executors.newFixedThreadPool(numThreads);

    int sharedNum = 0;
    final Node<T>[] nodeArray = new Node[addedNodes.size()];
    for (int i = 0; i < addedNodes.size(); i++) {
      final Node<T> node = addedNodes.get(i);
      nodeArray[i] = node;
      if (node.refCnt.get() == 0) {
        node.isNotShared = true;
      } else {
        sharedNum += 1;
      }
    }

    Arrays.parallelSort(nodeArray, new Comparator<Node<T>>() {
      @Override
      public int compare(final Node<T> o1, final Node<T> o2) {
        return (o1.refCnt.get() * o1.getDependencies().size() - o2.refCnt.get() * o2.getDependencies().size());
      }
    });

    final int pruningNum = (int)(sharedNum * (1-reuseRatio));

    int deletedNum = 0;
    for (int i = 0; i < addedNodes.size(); i++) {
      final Node<T> node = nodeArray[i];

      node.reset();

      if (!node.isNotShared && deletedNum < pruningNum) {
        node.isNotShared = true;
        deletedNum += 1;
      }
    }

    partialTimespans.reset();

    for (final Node<T> node : addedNodes) {
      executorService.submit(new Runnable() {
        @Override
        public void run() {
          addEdge(node);
        }
      });
    }

    executorService.shutdown();
    try {
      executorService.awaitTermination(1000, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
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

  /**
   * Add DependencyGraphNode and connect dependent nodes.
   */
  private void addOverlappingWindowNodeAndEdge(final double reuseRatio) {
    final ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
    final List<Node<T>> addedNodes = new LinkedList<>();


    final boolean[][] table = new boolean[(int)period + largestWindowSize][(int)period+largestWindowSize+1];

    for (int i = 0; i < period + largestWindowSize; i++) {
      for (int j = 0; j < period  + largestWindowSize + 1; j++) {
        table[i][j] = false;
      }
    }

    if (numThreads == 1) {
      for (final Timescale timescale : timescales) {

        for (long time = timescale.intervalSize + startTime; time <= period + startTime; time += timescale.intervalSize) {
          // create vertex and add it to the table cell of (time, windowsize)
          final long start = time - timescale.windowSize;
          final Node parent = new Node(start, time, false);
          addedNodes.add(parent);

          table[(int)start+largestWindowSize][(int)time+largestWindowSize] = true;

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

              table[(int)start+largestWindowSize][(int)time+largestWindowSize] = true;

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


    // Adjust dependency graph!
    if (reuseRatio < 1.0) {
      adjustDependencyGraph(reuseRatio, addedNodes);
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
