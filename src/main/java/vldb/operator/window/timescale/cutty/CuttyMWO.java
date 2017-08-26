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
package vldb.operator.window.timescale.cutty;

import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.remote.impl.Tuple2;
import vldb.evaluation.Metrics;
import vldb.operator.OutputEmitter;
import vldb.operator.window.aggregator.CAAggregator;
import vldb.operator.window.timescale.*;
import vldb.operator.window.timescale.common.DepOutputAndResult;
import vldb.operator.window.timescale.common.Timespan;
import vldb.operator.window.timescale.pafas.dynamic.WindowManager;
import vldb.operator.window.timescale.pafas.event.WindowTimeEvent;
import vldb.operator.window.timescale.parameter.StartTime;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * This executes incremental aggregation and slices the results.
 * @param <I> input
 * @param <V> aggregated result
 */
public final class CuttyMWO<I, V> implements TimescaleWindowOperator<I, V> {
  private static final Logger LOG = Logger.getLogger(CuttyMWO.class.getName());

  /**
   * Aggregator for incremental aggregation.
   */
  private final CAAggregator<I, V> aggregator;

  /**
   * A bucket for incremental aggregation.
   * It saves aggregated results for partial aggregation.
   */
  private V bucket;


  private final Metrics metrics;

  private final TimeMonitor timeMonitor;

  private final WindowManager windowManager;

  private final long startTime;

  private final Map<Timespan, Long> begins;

  private final Fat<V> fat;

  private long prevSliceTime;

  private long minBegins;

  private final TimeWindowOutputHandler<V, ?> outputHandler;

  /**
   * DefaultSlicedWindowOperatorImpl.
   * @param aggregator an aggregator for incremental aggregation
   * @param startTime a start time of the mts operator
   */
  @Inject
  private CuttyMWO(
      final CAAggregator<I, V> aggregator,
      final Metrics metrics,
      @Parameter(StartTime.class) final Long startTime,
      final WindowManager windowManager,
      final Fat<V> fat,
      final TimeMonitor timeMonitor,
      final TimeWindowOutputHandler<V, ?> outputHandler) {
    this.metrics = metrics;
    this.aggregator = aggregator;
    this.bucket = initBucket();
    this.startTime = startTime;
    this.timeMonitor = timeMonitor;
    this.windowManager = windowManager;
    this.prevSliceTime = startTime;
    this.fat = fat;
    this.minBegins = startTime;
    this.outputHandler = outputHandler;
    this.begins = new HashMap<>();
    // Initialize begins
    for (final Timescale timescale : windowManager.timescales) {
      if (timescale.windowSize > timescale.intervalSize) {
        long endTime = timescale.intervalSize + startTime;
        while (endTime - timescale.windowSize <= startTime) {
          begins.put(new Timespan(startTime, endTime, timescale), startTime);
          endTime += timescale.intervalSize;
        }
      }
    }
  }

  private V initBucket() {
    return aggregator.init();
  }

  private Tuple2<List<Timespan>, List<Timespan>> startAndEndWindows(final long time) {
    final List<Timespan> startWindows = new LinkedList<>();
    final List<Timespan> endWindows = new LinkedList<>();

    for (final Timescale timescale : windowManager.timescales) {
      final long tsStart = startTime - (timescale.windowSize - timescale.intervalSize);
      final long elapsedTime = time - tsStart;

      if (elapsedTime % timescale.intervalSize == 0) {
        startWindows.add(new Timespan(time, time + timescale.windowSize, timescale));
      }

      if ((time - startTime) % timescale.intervalSize == 0) {
        endWindows.add(new Timespan(Math.max(startTime, time - timescale.windowSize), time, timescale));
      }
    }

    return new Tuple2<>(startWindows, endWindows);
  }

  /**
   * Aggregates input into the current bucket.
   * @param val input
   */
  @Override
  public void execute(final I val) {
    if (val instanceof WindowTimeEvent) {
      final long tickTime = ((WindowTimeEvent) val).time;

      final Tuple2<List<Timespan>, List<Timespan>> windows = startAndEndWindows(tickTime);
      final List<Timespan> startWindows = windows.getT1();
      final List<Timespan> endWindows = windows.getT2();

      if (startWindows.size() > 0) {
        fat.append(new Timespan(prevSliceTime, tickTime, null), bucket);
        bucket = initBucket();

        for (final Timespan startWindow : startWindows) {
          begins.put(startWindow, startWindow.startTime);
          if (tickTime < minBegins) {
            minBegins = tickTime;
          }
        }
        prevSliceTime = tickTime;
      }

      for (final Timespan endWindow : endWindows) {
        final long stt = System.nanoTime();

        final long start = begins.get(endWindow);
        begins.remove(endWindow);
        fat.removeUpTo(minBegins); // gc
        final V agg = aggregator.rollup(fat.merge(start), bucket);

        final long ett = System.nanoTime();
        timeMonitor.finalTime += (ett - stt);

        outputHandler.execute(new TimescaleWindowOutput<V>(endWindow.timescale,
            new DepOutputAndResult<V>(0, agg),
            endWindow.startTime, endWindow.endTime, endWindow.startTime >= startTime));
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
  public void prepare(final OutputEmitter<TimescaleWindowOutput<V>> outputEmitter) {

  }

  @Override
  public void close() throws Exception {
  }

  @Override
  public void addWindow(final Timescale ts, final long time) {

  }

  @Override
  public void removeWindow(final Timescale ts, final long time) {

  }
}