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
import vldb.operator.window.timescale.TimeWindowOutputHandler;
import vldb.operator.window.timescale.TimescaleWindowOutput;
import vldb.operator.window.timescale.parameter.NumThreads;
import vldb.operator.window.timescale.parameter.StartTime;

import javax.inject.Inject;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

/**
 * This performs final aggregation.
 */
public final class DefaultFinalAggregator<V> implements FinalAggregator<V> {
  private static final Logger LOG = Logger.getLogger(DefaultFinalAggregator.class.getName());

  private final SpanTracker<V> spanTracker;

  /**
   * An output handler for window output.
   */
  private OutputEmitter<TimescaleWindowOutput<V>> emitter;

  private final TimeWindowOutputHandler<V, ?> outputHandler;

  private final AtomicBoolean closed = new AtomicBoolean(false);

  private final ParallelTreeAggregator<?, V> parallelAggregator;

  //private final ExecutorService executorService;

  private final ForkJoinPool forkJoinPool;

  private final long startTime;

  private final int numThreads;

  private final Comparator<Timespan> timespanComparator;

  private final long endTime;

  //private final ConcurrentMap<Timescale, ExecutorService> executorServiceMap;

  private final ExecutorService executorService;
  /**
   * Default overlapping window operator.
   * @param spanTracker a computation reuser for final aggregation
   */
  @Inject
  private DefaultFinalAggregator(final SpanTracker<V> spanTracker,
                                 final TimeWindowOutputHandler<V, ?> outputHandler,
                                 final CAAggregator<?, V> aggregateFunction,
                                 @Parameter(NumThreads.class) final int numThreads,
                                 @Parameter(StartTime.class) final long startTime,
                                 final SharedForkJoinPool sharedForkJoinPool,
                                 @Parameter(EndTime.class) final long endTime) {
    this.spanTracker = spanTracker;
    this.outputHandler = outputHandler;
    this.numThreads = numThreads;
    this.forkJoinPool = sharedForkJoinPool.getForkJoinPool();
    this.parallelAggregator = new ParallelTreeAggregator<>(numThreads, 4, aggregateFunction, forkJoinPool);
    //this.executorServiceMap = new ConcurrentHashMap<>();
    this.executorService = Executors.newFixedThreadPool(40);
    this.startTime = startTime;
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
    for (final Timespan timespan : finalTimespans) {
      //System.out.println("BEFORE_GET: " + timespan);
      if (timespan.endTime <= endTime) {
        executorService.submit(new Runnable() {
          @Override
          public void run() {
            final List<V> aggregates = spanTracker.getDependentAggregates(timespan);
            //System.out.println("AFTER_GET: " + timespan);
            //aggregationCounter.incrementFinalAggregation(timespan.endTime, (List<Map>)aggregates);
            //System.out.println("INC: " + timespan);
            try {
              final V finalResult = parallelAggregator.doParallelAggregation(aggregates);
              //System.out.println("PUT_TIMESPAN: " + timespan);
              spanTracker.putAggregate(finalResult, timespan);
              outputHandler.execute(new TimescaleWindowOutput<V>(timespan.timescale,
                  actualTriggerTime, new DepOutputAndResult<V>(aggregates.size(), finalResult),
                  timespan.startTime, timespan.endTime, timespan.startTime >= startTime));
            } catch (Exception e) {
              e.printStackTrace();
              System.out.println(e);
            }
          }
        });
      }
    }
  }

  @Override
  public void close() throws Exception {
    if (closed.compareAndSet(false, true)) {
      executorService.shutdownNow();
      forkJoinPool.shutdownNow();
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