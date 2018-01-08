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
import vldb.evaluation.Metrics;
import vldb.evaluation.parameter.EndTime;
import vldb.operator.OutputEmitter;
import vldb.operator.window.aggregator.CAAggregator;
import vldb.operator.window.timescale.TimeMonitor;
import vldb.operator.window.timescale.TimeWindowOutputHandler;
import vldb.operator.window.timescale.TimescaleWindowOutput;
import vldb.operator.window.timescale.common.DepOutputAndResult;
import vldb.operator.window.timescale.common.FinalAggregator;
import vldb.operator.window.timescale.common.SpanTracker;
import vldb.operator.window.timescale.common.Timespan;
import vldb.operator.window.timescale.pafas.DependencyGraph;
import vldb.operator.window.timescale.pafas.Node;
import vldb.operator.window.timescale.pafas.vldb2018.singlethread.MultiThreadFinalAggregator;
import vldb.operator.window.timescale.parameter.NumThreads;
import vldb.operator.window.timescale.parameter.StartTime;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

/**
 * This performs final aggregation.
 */
public final class MultiThreadImprovedFinalAggregator<V> implements FinalAggregator<V> {
  private static final Logger LOG = Logger.getLogger(MultiThreadImprovedFinalAggregator.class.getName());

  private final SpanTracker<List<V>> spanTracker;

  /**
   * An output handler for window output.
   */
  private OutputEmitter<TimescaleWindowOutput<V>> emitter;

  private final TimeWindowOutputHandler<V, ?> outputHandler;

  private final AtomicBoolean closed = new AtomicBoolean(false);

  //private final ExecutorService executorService;


  private final long startTime;

  private final int numThreads;

  private final Metrics metrics;

  private final Comparator<Timespan> timespanComparator;

  private final long endTime;

  private final CAAggregator<?, V> aggregateFunction;

  private final TimeMonitor timeMonitor;

  //private final ExecutorService executorService;

  private final ForkJoinPool forkJoinPool;
  private final DependencyGraph<V> dependencyGraph;

  private final int threshold;

  //private final ConcurrentMap<Timescale, ExecutorService> executorServiceMap;

  /**
   * Default overlapping window operator.
   * @param spanTracker a computation reuser for final aggregation
   */
  @Inject
  private MultiThreadImprovedFinalAggregator(final SpanTracker<List<V>> spanTracker,
                                             final DependencyGraph<V> dependencyGraph,
                                             final TimeWindowOutputHandler<V, ?> outputHandler,
                                             final CAAggregator<?, V> aggregateFunction,
                                             @Parameter(NumThreads.class) final int numThreads,
                                             @Parameter(StartTime.class) final long startTime,
                                             final Metrics metrics,
                                             final TimeMonitor timeMonitor,
                                             @Parameter(MultiThreadFinalAggregator.ParallelThreshold.class)
                                             final int threshold,
                                             @Parameter(EndTime.class) final long endTime) {
    LOG.info("START " + this.getClass());
    this.dependencyGraph = dependencyGraph;
    this.timeMonitor = timeMonitor;
    this.threshold = threshold;
    this.spanTracker = spanTracker;
    this.forkJoinPool = ForkJoinPool.commonPool();
    this.outputHandler = outputHandler;
    this.numThreads = numThreads;
    //this.executorService = Executors.newFixedThreadPool(numThreads);
    this.aggregateFunction = aggregateFunction;
    //this.executorServiceMap = new ConcurrentHashMap<>();
    this.startTime = startTime;
    this.metrics = metrics;
    this.endTime = endTime;
    this.timespanComparator = new TimespanComparator();
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("[OLO: ");
    sb.append("]");
    return sb.toString();
  }

  private List<Node<V>> getSortedTimespans(final Map<Long, Node<V>> finalTimespans) {
    final List<Node<V>> list = new ArrayList<>(finalTimespans.size());
    for (final Map.Entry<Long, Node<V>> ft : finalTimespans.entrySet()) {
      list.add(ft.getValue());
    }

    list.sort(new Comparator<Node<V>>() {
      @Override
      public int compare(final Node<V> o1, final Node<V> o2) {
        if (o1.start < o2.start) {
          return -1;
        } else {
          return 1;
        }
      }
    });

    return list;
  }

  @Override
  public void triggerFinalAggregation(final List<Timespan> finalTimespans,
                                      final long actualTriggerTime) {
    /*
    Collections.sort(finalTimespans, timespanComparator);
    final Map<Node<List<V>>, DependencyGroup> timespanGroupMap = new HashMap<>(finalTimespans.size());
    final List<DependencyGroup> groups = new LinkedList<>();

    final long st = System.nanoTime();

    // Split dependencies
    //System.out.println("TS: " + finalTimespans);
    int groupId = 0;
    for (final Timespan timespan : finalTimespans) {
      final Node<List<V>> myNode = dependencyGraph.getNode(timespan);
      final Node<List<V>> lastChildNode = myNode.lastChildNode;
      if (lastChildNode.end == timespan.endTime && !lastChildNode.partial) {
        // Add my node to the child group
        //System.out.println(myNode + " FIND CN: " + lastChildNode);
        final DependencyGroup childGroup = timespanGroupMap.get(lastChildNode);
        childGroup.add(myNode);
        timespanGroupMap.put(myNode, childGroup);
      } else {
        // Create my group
        DependencyGroup group = timespanGroupMap.get(myNode);
        if (group == null) {
          group = new DependencyGroup(groupId);
          timespanGroupMap.put(myNode, group);
          group.add(myNode);
          groups.add(group);
          //System.out.println("ADD NODE TO GROUP: " + myNode);
          groupId += 1;
        }
      }
    }

    final long gtEnd = System.nanoTime();
    timeMonitor.groupingTime += (gtEnd - st);

    //System.out.println("GROUPS" + groups);
    // Parallelize groups
    final CountDownLatch countDownLatch = new CountDownLatch(groups.size());
    //final List<ForkJoinTask<Integer>> forkJoinTasks = new ArrayList<>(groups.size());
    for (final DependencyGroup group : groups) {
      forkJoinPool.submit(new AggregateTask(group, countDownLatch, actualTriggerTime));
    }

    try {
      countDownLatch.await();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    */

    /*
    for (final ForkJoinTask<Integer> forkJoinTask : forkJoinTasks) {
      try {
        forkJoinTask.get();
      } catch (InterruptedException e) {
        e.printStackTrace();
      } catch (ExecutionException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }
    */

    /*
    // End aggregate
    final long et = System.nanoTime();
    timeMonitor.finalTime += (et - st);
    */
  }

  @Override
  public void triggerFinalAggregation(final Map<Long, Node<V>> nodes,
                                      final long endTime,
                                      final long actualTriggerTime) {
    final Map<Node<V>, Boolean> deletedMap = new HashMap<>();
    final List<Node<V>> sortedNodes = getSortedTimespans(nodes);
    final DAG<Node<V>, Integer> dependencyGraph = new AdjacentListDAG<>();

    while (!sortedNodes.isEmpty()) {
      Node<V> firstNode = sortedNodes.remove(0);
      dependencyGraph.addVertex(firstNode);

      Node<V> dependentNode = firstNode.dependentNode;

      while (!dependentNode.partial) {

        if (deletedMap.get(dependentNode) == null) {
          sortedNodes.remove(dependentNode);
          deletedMap.put(dependentNode, true);
          dependencyGraph.addVertex(dependentNode);
        }

        if (dependencyGraph.isAdjacent(dependentNode, firstNode)) {
          break;
        }

        dependencyGraph.addEdge(dependentNode, firstNode, 1);
        firstNode = dependentNode;
        dependentNode = dependentNode.dependentNode;
      }
    }

    // Root nodes are actually leaf nodes in aggregation trees
    // We percolate up the aggregations
    final List<ForkJoinTask<V>> forkJoinTasks  = new ArrayList<>( dependencyGraph.getRootVertices().size());
    for (final Node<V> rootNode : dependencyGraph.getRootVertices()) {
      forkJoinTasks.add(
          forkJoinPool.submit(new AggregateTask(dependencyGraph, rootNode, endTime, actualTriggerTime)));
    }

    for (final ForkJoinTask<V> forkJoinTask : forkJoinTasks) {
      forkJoinTask.join();
    }
  }

  @Override
  public void close() throws Exception {
    if (closed.compareAndSet(false, true)) {
      forkJoinPool.shutdown();
    }
  }

  public static class TimespanComparator implements Comparator<Timespan> {

    @Override
    public int compare(final Timespan o1, final Timespan o2) {
      if (o1.startTime > o2.startTime) {
        return -1;
      } else if (o1.startTime == o2.startTime) {
        return 0;
      } else {
        return 1;
      }
    }
  }

  final class RecursiveAggregate extends RecursiveTask<V> {

    private final List<V> aggregates;

    public RecursiveAggregate(final List<V> aggregates) {
      this.aggregates = aggregates;
    }

    private Collection<RecursiveAggregate> createSubTasks() {
      final List<RecursiveAggregate> dividedTasks = new ArrayList<>();
      dividedTasks.add(new RecursiveAggregate(
          aggregates.subList(0, aggregates.size() / 2)));
      dividedTasks.add(new RecursiveAggregate(
          aggregates.subList(aggregates.size() / 2, aggregates.size())));
      return dividedTasks;
    }

    @Override
    protected V compute() {
      try {
        if (aggregates.size() > threshold) {
          final Collection<V> agg = new ArrayList<>(2);
          final Collection<RecursiveAggregate> tasks = createSubTasks();
          for (final RecursiveAggregate task : tasks) {
            agg.add(task.join());
          }
          return aggregateFunction.aggregate(agg);
        } else {
          return aggregateFunction.aggregate(aggregates);
        }
      } catch (final Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }
  }

  final class AggregateTask extends RecursiveTask<V> {
    private final Node<V> node;
    private final long endTime;
    private final long actualTriggerTime;
    private final DAG<Node<V>, Integer> graph;

    public AggregateTask(final DAG<Node<V>, Integer> graph,
                         final Node<V> node,
                         final long endTime,
                         final long actualTriggerTime) {
      this.graph = graph;
      this.node = node;
      this.endTime = endTime;
      this.actualTriggerTime = actualTriggerTime;
    }

    @Override
    protected V compute() {
      try {
        final List<V> aggregates = new ArrayList<>(node.getDependencies().size());
        for (final Node<V> dependentNode : node.getDependencies()) {
          if (dependentNode.end <= endTime) {
            // Do not count first outgoing edge
            if (dependentNode.getOutput() == null) {
              throw new RuntimeException("The agg " + dependentNode + " should not be null at " + node);
            }

            aggregates.add(dependentNode.getOutput());
            dependentNode.decreaseRefCnt();

            if (dependentNode.getOutput() == null) {
              if (dependentNode.partial) {
                //System.out.println("Deleted " + dependentNode);
                metrics.storedPartial -= 1;
              } else if (dependentNode.intermediate) {
                metrics.storedInter -= 1;
              } else {
                metrics.storedFinal -= 1;
              }
            }
          }
        }

        // Aggregates in a single thread
        final V result = aggregateFunction.aggregate(aggregates);
        // TODO: work stealing
        //final ForkJoinTask<V> task = new RecursiveAggregate(aggregates).fork();
        //final V result = task.join();

        node.saveOutput(result);

        if (!node.intermediate) {
          outputHandler.execute(new TimescaleWindowOutput<V>(node.timescale,
              actualTriggerTime, new DepOutputAndResult<V>(aggregates.size(), result),
              endTime - (node.end - node.start), endTime, false));
        }


        final Map<Node<V>, Integer> edges = graph.getEdges(node);
        final List<ForkJoinTask> forkJoinTasks = new ArrayList<>(edges.size());
        for (final Node<V> parent : edges.keySet()) {
          forkJoinTasks.add(
              new AggregateTask(graph, parent, endTime, actualTriggerTime).fork());
        }

        for (final ForkJoinTask forkJoinTask : forkJoinTasks) {
          forkJoinTask.join();
        }

        return result;
      } catch (final Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }
  }

}