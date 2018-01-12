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
package vldb.operator.window.timescale.pafas.vldb2018.dynamic;

import org.apache.reef.tang.annotations.Parameter;
import vldb.evaluation.Metrics;
import vldb.operator.OutputEmitter;
import vldb.operator.common.NotFoundException;
import vldb.operator.window.aggregator.CAAggregator;
import vldb.operator.window.timescale.TimeMonitor;
import vldb.operator.window.timescale.Timescale;
import vldb.operator.window.timescale.TimescaleWindowOperator;
import vldb.operator.window.timescale.TimescaleWindowOutput;
import vldb.operator.window.timescale.common.FinalAggregator;
import vldb.operator.window.timescale.common.OutputLookupTable;
import vldb.operator.window.timescale.common.Timespan;
import vldb.operator.window.timescale.pafas.Node;
import vldb.operator.window.timescale.pafas.dynamic.DynamicDependencyGraph;
import vldb.operator.window.timescale.pafas.dynamic.DynamicPartialTimespans;
import vldb.operator.window.timescale.pafas.dynamic.WindowManager;
import vldb.operator.window.timescale.pafas.event.WindowTimeEvent;
import vldb.operator.window.timescale.parameter.NumThreads;
import vldb.operator.window.timescale.parameter.StartTime;

import javax.inject.Inject;
import java.util.Map;
import java.util.logging.Logger;

/**
 * This executes incremental aggregation and slices the results.
 * @param <I> input
 * @param <V> aggregated result
 */
public final class DynamicFastMWO<I, V> implements TimescaleWindowOperator<I, V> {
  private static final Logger LOG = Logger.getLogger(DynamicFastMWO.class.getName());

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

  private final FinalAggregator<V> finalAggregator;

  private long nextRealTime;

  private final Metrics metrics;

  private final TimeMonitor timeMonitor;

  private final int numThreads;

  private final DynamicPartialTimespans partialTimespans;

  private final DynamicDependencyGraph dependencyGraph;

  private final OutputLookupTable<Node<V>> outputLookupTable;

  private final WindowManager windowManager;

  /**
   * DefaultSlicedWindowOperatorImpl.
   * @param aggregator an aggregator for incremental aggregation
   * @param startTime a start time of the mts operator
   */
  @Inject
  private DynamicFastMWO(
      final CAAggregator<I, V> aggregator,
      final DynamicPartialTimespans partialTimespans,
      final FinalAggregator<V> finalAggregator,
      final DynamicDependencyGraph<V> dependencyGraph,
      final Metrics metrics,
      final WindowManager windowManager,
      final OutputLookupTable<Node<V>> outputLookupTable,
      @Parameter(NumThreads.class) final int numThreads,
      @Parameter(StartTime.class) final Long startTime,
      final TimeMonitor timeMonitor) {
    this.metrics = metrics;
    this.aggregator = aggregator;
    this.numThreads = numThreads;
    this.bucket = initBucket();
    this.partialTimespans = partialTimespans;
    this.prevSliceTime = startTime;
    this.windowManager = windowManager;
    this.outputLookupTable = outputLookupTable;
    this.dependencyGraph = dependencyGraph;
    this.nextSliceTime = partialTimespans.getNextSliceTime(startTime);
    this.finalAggregator = finalAggregator;
    this.timeMonitor = timeMonitor;
  }

  private boolean isSliceTime(final long time) {
    if (nextSliceTime == time) {
      return true;
    } else {
      return false;
    }
  }

  private V initBucket() {
    return aggregator.init();
  }

  private int hashing(final I val) {
    return Math.abs(val.hashCode()) %  numThreads;
  }

  /**
   * Aggregates input into the current bucket.
   * @param val input
   */
  @Override
  public void execute(final I val) {
    if (val instanceof WindowTimeEvent) {
      if (isSliceTime(((WindowTimeEvent) val).time)) {
        //System.out.println("SLICE: " + prevSliceTime + ", " + nextSliceTime);
        slice(prevSliceTime, nextSliceTime);
      }
    } else {
      final long st = System.nanoTime();
      aggregator.incrementalAggregate(bucket, val);
      final long et = System.nanoTime();
      timeMonitor.partialTime += (et - st);
      metrics.incrementPartial();
    }
  }

  private void putAggregate(final V agg, final Timespan timespan) {
    final Node<V> node = dependencyGraph.getNode(timespan);
    metrics.storedPartial += 1;
    node.saveOutput(agg);
  }

  private Map<Long, Node<V>> getFinalTimespans(final long t) {
    try {
      // incremental build
      dependencyGraph.getFinalTimespans(t);
      return outputLookupTable.lookup(t);
    } catch (final NotFoundException e) {
      return null;
    }
  }


  private void slice(final long prev, final long next) {
    final long statTime = System.currentTimeMillis();

    final V partialAggregation = bucket;
    bucket = initBucket();
    //System.out.println("PARTIAL_SIZE: " + ((Map)partialAggregation).size() + "\t" + (prevSliceTime) + "-" + (nextSliceTime));
    //System.out.println("SLICE: " + prev + ", " + next);
    prevSliceTime = next;
    nextSliceTime = partialTimespans.getNextSliceTime(prevSliceTime);
    //System.out.println("SLICE changed: " + prevSliceTime + ", " + nextSliceTime);
    putAggregate(partialAggregation, new Timespan(prev, next, null));

    final Map<Long, Node<V>> finalTimespans = getFinalTimespans(next);

    if (finalTimespans == null) {
      throw new RuntimeException("Null at " + next);
    }

    //System.out.println("final timespans at " + next  + ": " + finalTimespans);
    finalAggregator.triggerFinalAggregation(finalTimespans, next, statTime);
  }

  @Override
  public void prepare(final OutputEmitter<TimescaleWindowOutput<V>> outputEmitter) {

  }

  @Override
  public void close() throws Exception {
  }

  @Override
  public void addWindow(final Timescale window, final long time) {
    windowManager.addWindow(window, time);
    partialTimespans.addWindow(window, prevSliceTime, time);
    dependencyGraph.addSlidingWindow(window, time);

    // 1) Update next slice time
    //System.out.println("PREV SLICE: " + nextSliceTime + ", at " + time);
    if (prevSliceTime < time) {
      // slice here!
      slice(prevSliceTime, time);
    } else {
      nextSliceTime = partialTimespans.getNextSliceTime(time);
    }
  }

  @Override
  public void removeWindow(final Timescale window, final long time) {
    final long stTime = windowManager.timescaleStartTime(window);
    windowManager.removeWindow(window, time);
    partialTimespans.removeWindow(window, prevSliceTime, time, stTime);
    dependencyGraph.removeSlidingWindow(window, stTime, time);

    // 2) Update next slice time
    nextSliceTime = partialTimespans.getNextSliceTime(prevSliceTime);
    //System.out.println("prevslice : " + prevSliceTime + ", nextSlice: " + nextSliceTime);
  }
}