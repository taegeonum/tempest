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
package vldb.operator.window.timescale.flatfit;

import org.apache.reef.tang.annotations.Parameter;
import vldb.evaluation.Metrics;
import vldb.operator.OutputEmitter;
import vldb.operator.window.aggregator.CAAggregator;
import vldb.operator.window.timescale.*;
import vldb.operator.window.timescale.common.DepOutputAndResult;
import vldb.operator.window.timescale.common.Timespan;
import vldb.operator.window.timescale.pafas.PartialTimespans;
import vldb.operator.window.timescale.pafas.dynamic.WindowManager;
import vldb.operator.window.timescale.pafas.event.WindowTimeEvent;
import vldb.operator.window.timescale.parameter.StartTime;

import javax.inject.Inject;
import java.util.*;
import java.util.logging.Logger;

/**
 * This executes incremental aggregation and slices the results.
 * @param <I> input
 * @param <V> aggregated result
 */
public final class FlatFitMWO<I, V> implements TimescaleWindowOperator<I, V> {
  private static final Logger LOG = Logger.getLogger(FlatFitMWO.class.getName());

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

  private long prevSliceTime;

  private final TimeWindowOutputHandler<V, ?> outputHandler;

  private final PartialTimespans partialTimespans;

  private final int wSize;

  private final List<V> partials;

  private final List<Integer> pointers;

  private final List<Timespan> timespans;

  private final Stack<Integer> positions;

  private int currInd;

  private int prevInd;

  /**
   * Current slice time.
   */
  private long nextSliceTime;

  private final Map<Timescale, Integer> wSizeMap;

  /**
   * DefaultSlicedWindowOperatorImpl.
   * @param aggregator an aggregator for incremental aggregation
   * @param startTime a start time of the mts operator
   */
  @Inject
  private FlatFitMWO(
      final PartialTimespans partialTimespans,
      final CAAggregator<I, V> aggregator,
      final Metrics metrics,
      @Parameter(StartTime.class) final Long startTime,
      final WindowManager windowManager,
      final TimeMonitor timeMonitor,
      final TimeWindowOutputHandler<V, ?> outputHandler) {
    this.metrics = metrics;
    this.wSizeMap = new HashMap<>();
    this.partialTimespans = partialTimespans;
    this.aggregator = aggregator;
    this.bucket = initBucket();
    this.startTime = startTime;
    this.timeMonitor = timeMonitor;
    this.windowManager = windowManager;
    this.wSize = findWsize(windowManager.timescales);
    this.prevSliceTime = startTime;
    this.outputHandler = outputHandler;
    this.partials = new ArrayList<V>((int)wSize);
    this.pointers = new ArrayList<>(wSize);
    this.timespans = new ArrayList<>(wSize);
    this.positions = new Stack<>();
    this.nextSliceTime = partialTimespans.getNextSliceTime(startTime);

    // Initialize
    for (int i = 0; i < wSize; i++) {
      partials.add(aggregator.init());
      pointers.add(i + 1);
      timespans.add(new Timespan(0, 0, null));
    }
    pointers.set(wSize - 1, 0);
    currInd = 0;
    prevInd = wSize - 1;
  }

  private long getGcd(long a, long b) {
    while (b > 0) {
      final long temp = b;
      b = a % b; // % is remainder
      a = temp;
    }
    return a;
  }

  private int findWsize(final List<Timescale> timescales) {
    int max = 0;
    for (final Timescale timescale : timescales) {
      int count = 0;
      long t = startTime;
      while ((t = partialTimespans.getNextSliceTime(t)) <= timescale.windowSize + startTime) {
        count += 1;
      }
      wSizeMap.put(timescale, count);

      if (max < count) {
        max = count;
      }
    }

    return max;
  }


  private V initBucket() {
    return aggregator.init();
  }

  private boolean isSliceTime(final long time) {
    if (nextSliceTime == time) {
      return true;
    } else {
      return false;
    }
  }


  private final List<Timescale> getWindows(final long time) {
    final List<Timescale> endWindows = new LinkedList<>();
    for (int i = windowManager.timescales.size() - 1; i >= 0; i--) {
      final Timescale timescale = windowManager.timescales.get(i);
      if ((time - startTime) % timescale.intervalSize == 0) {
        endWindows.add(timescale);
      }
    }
    return endWindows;
  }

  /*
  private final List<Timescale> getWindows(final long time) {
    final List<Timescale> endWindows = new LinkedList<>();
    for (final Timescale timescale : windowManager.timescales) {
      if ((time - startTime) % timescale.intervalSize == 0) {
        endWindows.add(timescale);
      }
    }
    return endWindows;
  }
  */

  private Timespan mergeTs(final Timespan ts1, final Timespan ts2) {
    if (ts1.startTime < ts2.startTime) {
      return new Timespan(ts1.startTime, ts2.endTime, null);
    } else {
      return new Timespan(ts2.startTime, ts1.endTime, null);
    }
  }

  private void execution(final V newPartial, final long tickTime) {

    final long actualTriggerTime = System.currentTimeMillis();

    //System.out.println( "tickTime: " + tickTime);
    partials.set(prevInd, newPartial);
    pointers.set(prevInd, currInd);
    timespans.set(prevInd, new Timespan(tickTime - 1, tickTime, null));

    // get final timespans
    final List<Timescale> queriesToAnswer = getWindows(tickTime);
    for (final Timescale query : queriesToAnswer) {
      final List<Timespan> childNodes = new LinkedList<>();

      int startInd = currInd - wSizeMap.get(query);
      if (startInd < 0) {
        startInd += wSize;
      }

      do {
        positions.push(startInd);
        startInd = pointers.get(startInd);
      } while (startInd != currInd);

      final int ind = positions.pop();
      V answer = partials.get(ind);
      Timespan ts = timespans.get(ind);
      childNodes.add(ts);
      while (positions.size() > 1) {
        int tempInd = positions.pop();
        Timespan tempTs = timespans.get(tempInd);
        childNodes.add(tempTs);
        final List<V> l = new ArrayList<>(2);
        l.add(answer); l.add(partials.get(tempInd));
        answer = aggregator.aggregate(l);
        partials.set(tempInd, answer);
        pointers.set(tempInd, currInd);

        final Timespan newTs = mergeTs(ts, tempTs);
        //System.out.println("Store "  + newTs +
        //    ", for [" + (tickTime - query.windowSize) + ", " + tickTime + ")");

        timespans.set(tempInd, newTs);
      }

      if (positions.size() > 0) {
        final int uu = positions.pop();
        final List<V> l = new ArrayList<>(2);
        l.add(answer);
        l.add(partials.get(uu));
        answer = aggregator.aggregate(l);
        childNodes.add(timespans.get(uu));
      }

      final Iterator<Timespan> iter = childNodes.iterator();
      while (iter.hasNext()) {
        final Timespan tt = iter.next();
        if (tt.startTime == 0 && tt.endTime == 0) {
          iter.remove();
        }
      }

      //System.out.println("[" + (tickTime - query.windowSize) + ", " + tickTime + ") =>" + childNodes);

      outputHandler.execute(new TimescaleWindowOutput<V>(query,
              actualTriggerTime, new DepOutputAndResult<V>(0, answer),
          tickTime - query.windowSize, tickTime, true));
    }

    prevInd = currInd;
    currInd += 1;
    if (currInd == wSize) {
      currInd = 0;
    }
  }
  /**
   * Aggregates input into the current bucket.
   * @param val input
   */
  @Override
  public void execute(final I val) {
    if (val instanceof WindowTimeEvent) {
      final long tickTime = ((WindowTimeEvent) val).time;

      if (isSliceTime(tickTime)) {
        final V newPartial = bucket;
        bucket = aggregator.init();
        //System.out.println("PARTIAL_SIZE: " + ((Map)partialAggregation).size() + "\t" + (prevSliceTime) + "-" + (nextSliceTime));
        final long next = nextSliceTime;
        final long prev = prevSliceTime;
        prevSliceTime = nextSliceTime;
        nextSliceTime = partialTimespans.getNextSliceTime(prevSliceTime);

        // do execution
        execution(newPartial, tickTime);
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