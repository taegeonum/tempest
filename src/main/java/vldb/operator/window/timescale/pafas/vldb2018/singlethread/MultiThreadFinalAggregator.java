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
package vldb.operator.window.timescale.pafas.vldb2018.singlethread;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;
import vldb.evaluation.Metrics;
import vldb.evaluation.parameter.EndTime;
import vldb.operator.OutputEmitter;
import vldb.operator.window.aggregator.CAAggregator;
import vldb.operator.window.timescale.TimeMonitor;
import vldb.operator.window.timescale.TimeWindowOutputHandler;
import vldb.operator.window.timescale.TimescaleWindowOutput;
import vldb.operator.window.timescale.common.*;
import vldb.operator.window.timescale.pafas.DependencyGraph;
import vldb.operator.window.timescale.pafas.Node;
import vldb.operator.window.timescale.parameter.NumThreads;
import vldb.operator.window.timescale.parameter.StartTime;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

/**
 * This performs final aggregation.
 */
public final class MultiThreadFinalAggregator<V> implements FinalAggregator<V> {
  private static final Logger LOG = Logger.getLogger(MultiThreadFinalAggregator.class.getName());

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
  private final ParallelTreeAggregator<?, V> parallelAggregator;
  private final DependencyGraph<V> dependencyGraph;

  private final int threshold;

  //private final ConcurrentMap<Timescale, ExecutorService> executorServiceMap;

  @NamedParameter(short_name = "pt", default_value = "100")
  public static final class ParallelThreshold implements Name<Integer> {
  }

  /**
   * Default overlapping window operator.
   * @param spanTracker a computation reuser for final aggregation
   */
  @Inject
  private MultiThreadFinalAggregator(final SpanTracker<List<V>> spanTracker,
                                     final DependencyGraph<V> dependencyGraph,
                                     final TimeWindowOutputHandler<V, ?> outputHandler,
                                     final CAAggregator<?, V> aggregateFunction,
                                     @Parameter(NumThreads.class) final int numThreads,
                                     @Parameter(StartTime.class) final long startTime,
                                     final Metrics metrics,
                                     final TimeMonitor timeMonitor,
                                     @Parameter(ParallelThreshold.class) final int threshold,
                                     @Parameter(EndTime.class) final long endTime) {
    LOG.info("START " + this.getClass());
    this.dependencyGraph = dependencyGraph;
    this.timeMonitor = timeMonitor;
    this.threshold = threshold;
    this.spanTracker = spanTracker;
    this.forkJoinPool = ForkJoinPool.commonPool();
    this.outputHandler = outputHandler;
    this.parallelAggregator = new ParallelTreeAggregator<>(numThreads, 30 * numThreads, aggregateFunction, forkJoinPool);
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
    final List<Node<V>> sortedNodes = getSortedTimespans(nodes);
    final List<Node<V>> independentNodes = new LinkedList<>();

    while (!sortedNodes.isEmpty()) {
      final Node<V> firstNode = sortedNodes.remove(0);
      independentNodes.add(firstNode);

      Node<V> dependentNode = firstNode.dependentNode;

      while (!dependentNode.partial) {
        sortedNodes.remove(dependentNode);
        dependentNode = dependentNode.dependentNode;
      }
    }


    for (final Node<V> independentNode : independentNodes) {
      // compute aggregation
      forkJoinPool.submit(new AggregateTask(independentNode, endTime, actualTriggerTime));
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

    public AggregateTask(final Node<V> node, final long endTime,
                         final long actualTriggerTime) {
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
            dependentNode.lock.lock();
            if (dependentNode.getOutput() == null) {
              // recursive computation
              aggregates.add(
                  new AggregateTask(dependentNode, endTime, actualTriggerTime).fork().join());
            } else {
              aggregates.add(dependentNode.getOutput());
            }
            dependentNode.lock.unlock();

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

        final ForkJoinTask<V> task = new RecursiveAggregate(aggregates).fork();
        final V result = task.join();
        node.saveOutput(result);

        if (!node.intermediate) {
          outputHandler.execute(new TimescaleWindowOutput<V>(node.timescale,
              actualTriggerTime, new DepOutputAndResult<V>(aggregates.size(), result),
              endTime - (node.end - node.start), endTime, false));
        }

        return result;
      } catch (final Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }
  }

}