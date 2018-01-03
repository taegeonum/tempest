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
package vldb.operator.window.timescale.common;

import org.apache.reef.tang.annotations.Parameter;
import vldb.evaluation.Metrics;
import vldb.evaluation.parameter.EndTime;
import vldb.operator.OutputEmitter;
import vldb.operator.window.aggregator.CAAggregator;
import vldb.operator.window.timescale.TimeMonitor;
import vldb.operator.window.timescale.TimeWindowOutputHandler;
import vldb.operator.window.timescale.TimescaleWindowOutput;
import vldb.operator.window.timescale.pafas.Node;
import vldb.operator.window.timescale.pafas.dynamic.DynamicDependencyGraph;
import vldb.operator.window.timescale.parameter.NumThreads;
import vldb.operator.window.timescale.parameter.StartTime;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
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
  private final DynamicDependencyGraph<List<V>> dependencyGraph;

  //private final ConcurrentMap<Timescale, ExecutorService> executorServiceMap;

  /**
   * Default overlapping window operator.
   * @param spanTracker a computation reuser for final aggregation
   */
  @Inject
  private MultiThreadFinalAggregator(final SpanTracker<List<V>> spanTracker,
                                     final DynamicDependencyGraph<List<V>> dependencyGraph,
                                     final TimeWindowOutputHandler<V, ?> outputHandler,
                                     final CAAggregator<?, V> aggregateFunction,
                                     @Parameter(NumThreads.class) final int numThreads,
                                     @Parameter(StartTime.class) final long startTime,
                                     final Metrics metrics,
                                     final TimeMonitor timeMonitor,
                                     final SharedForkJoinPool sharedForkJoinPool,
                                     @Parameter(EndTime.class) final long endTime) {
    LOG.info("START " + this.getClass());
    this.dependencyGraph = dependencyGraph;
    this.timeMonitor = timeMonitor;
    this.spanTracker = spanTracker;
    this.forkJoinPool = sharedForkJoinPool.getForkJoinPool();
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

  @Override
  public void triggerFinalAggregation(final List<Timespan> finalTimespans,
                                      final long actualTriggerTime) {
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

    // End aggregate
    final long et = System.nanoTime();
    timeMonitor.finalTime += (et - st);
  }

  @Override
  public void triggerFinalAggregation(final Map<Long, Node<V>> nodes, final long endTime, final long actualTriggerTime) {
    throw new RuntimeException("Not supported");
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

  final class AggregateTask extends ForkJoinTask<Integer> {
    private Integer result;

    private final DependencyGroup group;
    private final CountDownLatch countDownLatch;
    private final long actualTriggerTime;

    public AggregateTask(final DependencyGroup group,
                         final CountDownLatch countDownLatch,
                         final long actualTriggerTime) {
      this.group = group;
      this.countDownLatch = countDownLatch;
      this.actualTriggerTime = actualTriggerTime;
    }
    @Override
    public Integer getRawResult() {
      return result;
    }

    @Override
    protected void setRawResult(final Integer value) {
      this.result = value;
    }

    @Override
    protected boolean exec() {
      for (final Node<List<V>> myNode : group.group) {
        final Timespan timespan = new Timespan(myNode.start, myNode.end, myNode.timescale);
        final List<List<V>> aggregates = spanTracker.getDependentAggregates(timespan);
        // Start aggregate
        final List<ForkJoinTask<V>> forkJoinTasks = new ArrayList<>(numThreads);
        for (int i = 0; i < numThreads; i++) {
          forkJoinTasks.add(new Task2(i, aggregates).fork());
        }
        final List<V> finalResult = new ArrayList<>(numThreads);
        for (final ForkJoinTask<V> forkJoinTask : forkJoinTasks) {
          finalResult.add(forkJoinTask.join());
        }
        spanTracker.putAggregate(finalResult, timespan);
        outputHandler.execute(new TimescaleWindowOutput<V>(timespan.timescale,
            actualTriggerTime, new DepOutputAndResult<V>(aggregates.size(), finalResult.get(0)),
            timespan.startTime, timespan.endTime, timespan.startTime >= startTime));
      }
      setRawResult(1);
      countDownLatch.countDown();
      return true;
    }
  }

  final class Task2 extends ForkJoinTask<V> {

    private final int index;
    private final List<List<V>> aggregates;

    private V result;

    public Task2(final int index,
                 final List<List<V>> aggregates) {
      this.index = index;
      this.aggregates = aggregates;
    }

    @Override
    public V getRawResult() {
      return result;
    }

    @Override
    protected void setRawResult(final V value) {
      this.result = value;
    }

    @Override
    protected boolean exec() {
      final List<V> subAggregates = new ArrayList<V>(aggregates.size());
      for (int j = 0; j < aggregates.size(); j++) {
        subAggregates.add(aggregates.get(j).get(index));
      }
      final V result = aggregateFunction.aggregate(subAggregates);
      //final V result = parallelAggregator.doParallelAggregation(subAggregates);
      setRawResult(result);
      return true;
    }
  }

  final class DependencyGroup {
    private final int id;
    public final List<Node<List<V>>> group;
    public DependencyGroup(final int id) {
      this.id = id;
      this.group = new LinkedList<>();
    }

    public void add(final Node<List<V>> timespan) {
      group.add(timespan);
    }

    @Override
    public String toString() {
      return "GID: " + id + ", " + group;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      final DependencyGroup that = (DependencyGroup) o;

      if (id != that.id) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return id;
    }
  }
}