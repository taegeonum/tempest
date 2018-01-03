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
import vldb.operator.window.timescale.parameter.NumThreads;
import vldb.operator.window.timescale.parameter.StartTime;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

/**
 * This performs final aggregation.
 */
public final class SingleThreadFinalAggregator<V> implements FinalAggregator<V> {
  private static final Logger LOG = Logger.getLogger(SingleThreadFinalAggregator.class.getName());

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

  private final SpanTracker<V> spanTracker;

  //private final ConcurrentMap<Timescale, ExecutorService> executorServiceMap;

  /**
   * Default overlapping window operator.
   */
  @Inject
  private SingleThreadFinalAggregator(final TimeWindowOutputHandler<V, ?> outputHandler,
                                      final CAAggregator<?, V> aggregateFunction,
                                      @Parameter(NumThreads.class) final int numThreads,
                                      @Parameter(StartTime.class) final long startTime,
                                      final Metrics metrics,
                                      final TimeMonitor timeMonitor,
                                      final SpanTracker<V> spanTracker,
                                      @Parameter(EndTime.class) final long endTime) {
    LOG.info("START " + this.getClass());
    this.timeMonitor = timeMonitor;
    this.outputHandler = outputHandler;
    this.numThreads = numThreads;
    this.aggregateFunction = aggregateFunction;
    //this.executorServiceMap = new ConcurrentHashMap<>();
    this.startTime = startTime;
    this.metrics = metrics;
    this.endTime = endTime;
    this.spanTracker = spanTracker;
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
        if (o1.start <= o2.start) {
          return 1;
        } else {
          return -1;
        }
      }
    });

    return list;
  }

  private List<V> buildAggregate(final Node<V> node, final long endTime) {
    final List<Node<V>> dependentNodes = node.getDependencies();
    //System.out.println(timespan + "=>" + dependentNodes);

    //System.out.println(timespan + " DEP_NODES: " + dependentNodes);
    final List<V> aggregates = new ArrayList<>(dependentNodes.size());
    for (final Node<V> dependentNode : dependentNodes) {
      if (dependentNode.end <= endTime) {
        // Do not count first outgoing edge

        if (dependentNode.getOutput() == null) {
          throw new RuntimeException("null aggregate at: " + dependentNode + ", when generating [" + endTime + ", " + (endTime - (node.end - node.start)) + ")");
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

    return aggregates;
  }

  private void putAggregate(final V agg, final Node<V> node) {
    if (node.getInitialRefCnt() != 0) {
      if (node.partial) {
        //System.out.println("Added " + node);
        metrics.storedPartial += 1;
      } else {
        metrics.storedFinal += 1;
      }
      node.saveOutput(agg);
    }
  }

  @Override
  public void triggerFinalAggregation(final List<Timespan> finalTimespans,
                                      final long actualTriggerTime) {
    Collections.sort(finalTimespans, timespanComparator);
    for (final Timespan timespan : finalTimespans) {
      //System.out.println("BEFORE_GET: " + timespan);
      //if (timespan.endTime <= endTime) {
      final List<V> aggregates = spanTracker.getDependentAggregates(timespan);
      //System.out.println("AFTER_GET: " + timespan);
      //aggregationCounter.incrementFinalAggregation(timespan.endTime, (List<Map>)aggregates);
      //System.out.println("INC: " + timespan);
      try {
        //System.out.println("FINAL: (" + timespan.startTime + ", " + timespan.endTime + ")");
        // Calculate elapsed time
        final long st = System.nanoTime();
        final V finalResult = aggregateFunction.aggregate(aggregates);
        final long et = System.nanoTime();
        timeMonitor.finalTime += (et - st);
        //System.out.println("PUT_TIMESPAN: " + timespan);
        spanTracker.putAggregate(finalResult, timespan);
        outputHandler.execute(new TimescaleWindowOutput<V>(timespan.timescale,
            actualTriggerTime, new DepOutputAndResult<V>(aggregates.size(), finalResult),
            timespan.startTime, timespan.endTime, timespan.startTime >= startTime));
      } catch (Exception e) {
        e.printStackTrace();
        System.out.println(e);
      }
      //}
    }
  }


  @Override
  public void triggerFinalAggregation(final Map<Long, Node<V>> finalTimespans,
                                      final long endTime,
                                      final long actualTriggerTime) {
    final List<Node<V>> sortedTs = getSortedTimespans(finalTimespans);
    for (final Node<V> node : sortedTs) {
        final List<V> aggregates = buildAggregate(node, endTime);
        try {
          final long st = System.nanoTime();
          final V finalResult = aggregateFunction.aggregate(aggregates);
          final long et = System.nanoTime();
          timeMonitor.finalTime += (et - st);
          putAggregate(finalResult, node);

          if (!node.intermediate) {
            outputHandler.execute(new TimescaleWindowOutput<V>(node.timescale,
                actualTriggerTime, new DepOutputAndResult<V>(aggregates.size(), finalResult),
                endTime - (node.end - node.start), endTime, false));
          }
        } catch (Exception e) {
          e.printStackTrace();
          System.out.println(e);
          throw new RuntimeException(e);
        }
      //}
    }
  }

  @Override
  public void close() throws Exception {
    if (closed.compareAndSet(false, true)) {
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
}