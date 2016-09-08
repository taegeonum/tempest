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
import vldb.evaluation.parameter.EndTime;
import vldb.operator.OutputEmitter;
import vldb.operator.window.aggregator.CAAggregator;
import vldb.operator.window.timescale.TimeMonitor;
import vldb.operator.window.timescale.TimeWindowOutputHandler;
import vldb.operator.window.timescale.TimescaleWindowOutput;
import vldb.operator.window.timescale.parameter.NumThreads;
import vldb.operator.window.timescale.parameter.StartTime;
import vldb.operator.window.timescale.profiler.AggregationCounter;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
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

  private final AggregationCounter aggregationCounter;

  private final Comparator<Timespan> timespanComparator;

  private final long endTime;

  private final CAAggregator<?, V> aggregateFunction;

  private final TimeMonitor timeMonitor;

  //private final ExecutorService executorService;

  private final ForkJoinPool forkJoinPool;
  private final ParallelTreeAggregator<?, V> parallelAggregator;

  //private final ConcurrentMap<Timescale, ExecutorService> executorServiceMap;

  /**
   * Default overlapping window operator.
   * @param spanTracker a computation reuser for final aggregation
   */
  @Inject
  private MultiThreadFinalAggregator(final SpanTracker<List<V>> spanTracker,
                                     final TimeWindowOutputHandler<V, ?> outputHandler,
                                     final CAAggregator<?, V> aggregateFunction,
                                     @Parameter(NumThreads.class) final int numThreads,
                                     @Parameter(StartTime.class) final long startTime,
                                     final AggregationCounter aggregationCounter,
                                     final TimeMonitor timeMonitor,
                                     final SharedForkJoinPool sharedForkJoinPool,
                                     @Parameter(EndTime.class) final long endTime) {
    LOG.info("START " + this.getClass());
    this.timeMonitor = timeMonitor;
    this.spanTracker = spanTracker;
    this.forkJoinPool = sharedForkJoinPool.getForkJoinPool();
    this.outputHandler = outputHandler;
    this.parallelAggregator = new ParallelTreeAggregator<>(numThreads, 50, aggregateFunction, forkJoinPool);
    this.numThreads = numThreads;
    //this.executorService = Executors.newFixedThreadPool(numThreads);
    this.aggregateFunction = aggregateFunction;
    //this.executorServiceMap = new ConcurrentHashMap<>();
    this.startTime = startTime;
    this.aggregationCounter = aggregationCounter;
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
  public void triggerFinalAggregation(final List<Timespan> finalTimespans) {
    Collections.sort(finalTimespans, timespanComparator);
    for (final Timespan timespan : finalTimespans) {
      //System.out.println("BEFORE_GET: " + timespan);
      //if (timespan.endTime <= endTime) {
      final List<List<V>> aggregates = spanTracker.getDependentAggregates(timespan);
      final List<V> finalResult = new LinkedList<>();
      final List<Future<V>> futures = new LinkedList<>();
      final int size = aggregates.size();

      // Start aggregate
      final long st = System.nanoTime();

      for (int i = 0; i < numThreads; i++) {
        final int index = i;
        futures.add(forkJoinPool.submit(new Callable<V>() {
          public V call() {
            final List<V> subAggregates = new LinkedList<V>();
            for (int j = 0; j < size; j++) {
              subAggregates.add(aggregates.get(j).get(index));
            }
            //return parallelAggregator.doParallelAggregation(subAggregates);
            return aggregateFunction.aggregate(subAggregates);
          }
        }));
      }

      for (final Future<V> future : futures) {
        try {
          finalResult.add(future.get());
        } catch (InterruptedException e) {
          e.printStackTrace();
        } catch (ExecutionException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      }

      // End aggregate
      final long et = System.nanoTime();
      timeMonitor.finalTime += (et - st);

      long numKey = 0;
      for (final V agg : finalResult) {
        numKey += ((Map)agg).size();
      }
      timeMonitor.storedKey += numKey;
      spanTracker.putAggregate(finalResult, timespan);
      outputHandler.execute(new TimescaleWindowOutput<V>(timespan.timescale,
          new DepOutputAndResult<V>(aggregates.size(), finalResult.get(0)),
          timespan.startTime, timespan.endTime, timespan.startTime >= startTime));
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
}