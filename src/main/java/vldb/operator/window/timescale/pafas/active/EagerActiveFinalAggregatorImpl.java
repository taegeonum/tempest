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
import vldb.evaluation.Metrics;
import vldb.evaluation.parameter.EndTime;
import vldb.operator.OutputEmitter;
import vldb.operator.window.aggregator.CAAggregator;
import vldb.operator.window.timescale.TimeWindowOutputHandler;
import vldb.operator.window.timescale.TimescaleWindowOutput;
import vldb.operator.window.timescale.common.DepOutputAndResult;
import vldb.operator.window.timescale.common.SpanTracker;
import vldb.operator.window.timescale.common.Timespan;
import vldb.operator.window.timescale.parameter.NumThreads;
import vldb.operator.window.timescale.parameter.StartTime;

import javax.inject.Inject;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

/**
 * This performs final aggregation.
 */
public final class EagerActiveFinalAggregatorImpl<V> implements ActiveFinalAggregator<V> {
  private static final Logger LOG = Logger.getLogger(EagerActiveFinalAggregatorImpl.class.getName());

  private final SpanTracker<V> spanTracker;

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

  //private final ConcurrentMap<Timescale, ExecutorService> executorServiceMap;

  /**
   * Default overlapping window operator.
   * @param spanTracker a computation reuser for final aggregation
   */
  @Inject
  private EagerActiveFinalAggregatorImpl(final SpanTracker<V> spanTracker,
                                         final TimeWindowOutputHandler<V, ?> outputHandler,
                                         final CAAggregator<?, V> aggregateFunction,
                                         @Parameter(NumThreads.class) final int numThreads,
                                         @Parameter(StartTime.class) final long startTime,
                                         final Metrics metrics,
                                         @Parameter(EndTime.class) final long endTime) {
    LOG.info("START " + this.getClass());
    this.spanTracker = spanTracker;
    this.outputHandler = outputHandler;
    this.numThreads = numThreads;
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
  public void triggerFinalAggregation(final List<Timespan> finalTimespans) {
    Collections.sort(finalTimespans, timespanComparator);
    for (final Timespan timespan : finalTimespans) {
      //System.out.println("BEFORE_GET: " + timespan);
      //if (timespan.endTime <= endTime) {
        final List<V> aggregates = spanTracker.getDependentAggregates(timespan);
      final V finalResult = aggregates.get(0);

      spanTracker.putAggregate(finalResult, timespan);
      outputHandler.execute(new TimescaleWindowOutput<V>(timespan.timescale,
          new DepOutputAndResult<V>(aggregates.size(), finalResult),
          timespan.startTime, timespan.endTime, timespan.startTime >= startTime));
      //}
    }
  }

  @Override
  public void triggerFinalAggregation(final List<Timespan> finalTimespans, final V activePartial) {
    Collections.sort(finalTimespans, timespanComparator);
    for (final Timespan timespan : finalTimespans) {
      //System.out.println("BEFORE_GET: " + timespan);
      //if (timespan.endTime <= endTime) {
      final List<V> aggregates = spanTracker.getDependentAggregates(timespan);
      final V agg = aggregates.get(0);
      final V finalResult = aggregateFunction.rollup(agg, activePartial);
      spanTracker.putAggregate(finalResult, timespan);
      outputHandler.execute(new TimescaleWindowOutput<V>(timespan.timescale,
          new DepOutputAndResult<V>(aggregates.size(), finalResult),
          timespan.startTime, timespan.endTime, timespan.startTime >= startTime));
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