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

import org.apache.reef.tang.annotations.Parameter;
import vldb.evaluation.Metrics;
import vldb.operator.common.NotFoundException;
import vldb.operator.window.aggregator.CAAggregator;
import vldb.operator.window.timescale.TimeMonitor;
import vldb.operator.window.timescale.common.*;
import vldb.operator.window.timescale.pafas.DependencyGraph;
import vldb.operator.window.timescale.pafas.Node;
import vldb.operator.window.timescale.pafas.PeriodCalculator;
import vldb.operator.window.timescale.pafas.event.WindowTimeEvent;
import vldb.operator.window.timescale.parameter.StartTime;

import javax.inject.Inject;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This executes incremental aggregation and slices the results.
 * @param <I> input
 * @param <V> aggregated result
 */
public final class SingleThreadPartialAggregator<I, V> implements PartialAggregator<I> {
  private static final Logger LOG = Logger.getLogger(SingleThreadPartialAggregator.class.getName());

  /**
   * Aggregator for incremental aggregation.
   */
  private final CAAggregator<I, V> aggregator;

  /**
   * Previous slice time.
   */
  private long prevSliceTime;

  /**
   * Current slice time.
   */
  private long nextSliceTime;

  /**
   * A bucket for incremental aggregation.
   * It saves aggregated results for partial aggregation.
   */
  private V bucket;

  private final SpanTracker<V> spanTracker;

  private final FinalAggregator<V> finalAggregator;

  private long nextRealTime;

  private final Metrics metrics;

  private final TimeMonitor timeMonitor;

  private final DependencyGraph<V> dependencyGraph;

  private final OutputLookupTable<Node<V>> outputLookupTable;
  private final long period;

  private final long startTime;
  /**
   * DefaultSlicedWindowOperatorImpl.
   * @param aggregator an aggregator for incremental aggregation
   * @param startTime a start time of the mts operator
   */
  @Inject
  private SingleThreadPartialAggregator(
      final CAAggregator<I, V> aggregator,
      final SpanTracker<V> spanTracker,
      final FinalAggregator<V> finalAggregator,
      final Metrics metrics,
      final PeriodCalculator periodCalculator,
      final DependencyGraph<V> dependencyGraph,
      final OutputLookupTable<Node<V>> outputLookupTable,
      @Parameter(StartTime.class) final Long startTime,
      final TimeMonitor timeMonitor) {
    this.timeMonitor = timeMonitor;
    this.metrics = metrics;
    this.aggregator = aggregator;
    this.period = periodCalculator.getPeriod();
    this.bucket = aggregator.init();
    this.prevSliceTime = startTime;
    this.spanTracker = spanTracker;
    this.finalAggregator = finalAggregator;
    this.dependencyGraph = dependencyGraph;
    this.startTime = startTime;
    this.outputLookupTable = outputLookupTable;
    this.nextSliceTime = spanTracker.getNextSliceTime(startTime);
    this.nextRealTime = System.currentTimeMillis() + (nextSliceTime - prevSliceTime) * 1000;
  }

  private boolean isSliceTime(final long time) {
    if (nextSliceTime == time) {
      return true;
    } else {
      return false;
    }
  }

  private void putAggregate(final V agg, final Timespan timespan) {
    final Node<V> node = dependencyGraph.getNode(timespan);
    metrics.storedPartial += 1;
    node.saveOutput(agg);
  }
  private long adjEndTime(final long time) {
    final long adj = (time - startTime) % period == 0 ? (startTime + period) : startTime + (time - startTime) % period;
    LOG.log(Level.FINE, "adjEndTime: time: " + time + ", adj: " + adj);
    return adj;
  }

  private Map<Long, Node<V>> getFinalTimespans(final long t) {
    final long et = adjEndTime(t);
    try {
      return outputLookupTable.lookup(et);
    } catch (final NotFoundException e) {
      return null;
    }
  }

  /**
   * Aggregates input into the current bucket.
   * @param val input
   */
  @Override
  public void execute(final I val) {
    LOG.log(Level.FINE, "SlicedWindow aggregates input of [" +  val + "]");
    if (val instanceof WindowTimeEvent) {
      final long tickTime = ((WindowTimeEvent) val).time;
      if (isSliceTime(tickTime)) {
        final long statTime = System.currentTimeMillis();

        final V partialAggregation = bucket;
        bucket = aggregator.init();
        //System.out.println("PARTIAL_SIZE: " + ((Map)partialAggregation).size() + "\t" + (prevSliceTime) + "-" + (nextSliceTime));
        final long next = nextSliceTime;
        final long prev = prevSliceTime;
        prevSliceTime = nextSliceTime;
        nextSliceTime = spanTracker.getNextSliceTime(prevSliceTime);
        putAggregate(partialAggregation, new Timespan(prev, next, null));

        final Map<Long, Node<V>> finalTimespans = getFinalTimespans(next);

        if (finalTimespans == null) {
          throw new RuntimeException("Null at " + next);
        }
        // Aggregate
        finalAggregator.triggerFinalAggregation(finalTimespans, tickTime, statTime);
      } else {
        // perform intermediate aggregation
        final Map<Long, Node<V>> finalTimespans = getFinalTimespans(tickTime);
        if (finalTimespans != null) {
          final long statTime = System.currentTimeMillis();
          finalAggregator.triggerFinalAggregation(finalTimespans, tickTime, statTime);
        }
      }
    } else {
      final long st = System.nanoTime();
      aggregator.incrementalAggregate(bucket, val);
      final long et = System.nanoTime();
      timeMonitor.partialTime += (et - st);
      metrics.incrementPartial();
    }
  }

  @Override
  public void close() throws Exception {
  }
}