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
package vldb.operator.window.timescale.pafas.dynamic;

import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;
import vldb.operator.OutputEmitter;
import vldb.operator.window.aggregator.impl.CountByKeyAggregator;
import vldb.operator.window.timescale.TimeWindowOutputHandler;
import vldb.operator.window.timescale.Timescale;
import vldb.operator.window.timescale.TimescaleWindowOperator;
import vldb.operator.window.timescale.TimescaleWindowOutput;
import vldb.operator.window.timescale.pafas.event.WindowTimeEvent;
import vldb.operator.window.timescale.parameter.NumThreads;
import vldb.operator.window.timescale.parameter.StartTime;
import vldb.operator.window.timescale.parameter.TimescaleString;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

/**
 * This executes incremental aggregation and slices the results.
 * @param <I> input
 * @param <V> aggregated result
 */
public final class ScalableDynamicMWO<I, V> implements TimescaleWindowOperator<I, V> {
  private static final Logger LOG = Logger.getLogger(ScalableDynamicMWO.class.getName());

  private final int numThreads;

  private final List<DynamicMWO<I, V>> dynamicMWOList;
  private final List<Thread> threads;
  private final List<BlockingQueue<I>> queueList = new LinkedList<>();
  private final List<AtomicBoolean> closedList = new LinkedList<>();

  private final CountDownLatch endCountDown;

  /**
   * DefaultSlicedWindowOperatorImpl.
   * @param startTime a start time of the mts operator
   */
  @Inject
  private ScalableDynamicMWO(
      @Parameter(TimescaleString.class) final String timescaleString,
      @Parameter(StartTime.class) final Long startTime,
      final TimeWindowOutputHandler<I, V> timeWindowOutputHandler,
      @Parameter(NumThreads.class) final int numThreads) throws InjectionException, InterruptedException {
    this.dynamicMWOList = new LinkedList<>();
    this.numThreads = numThreads;
    this.threads = new LinkedList<>();
    this.endCountDown = new CountDownLatch(numThreads);

    final CountDownLatch countDownLatch = new CountDownLatch(numThreads);
    for (int i = 0; i < numThreads; i++) {
      final int index = i;
      final Configuration conf = DynamicMWOConfiguration.CONF
          .set(DynamicMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
          .set(DynamicMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
          .set(DynamicMWOConfiguration.SELECTION_ALGORITHM, DynamicDPSelectionAlgorithm.class)
          .set(DynamicMWOConfiguration.OUTPUT_LOOKUP_TABLE, DynamicDPOutputLookupTableImpl.class)
          .set(DynamicMWOConfiguration.DYNAMIC_DEPENDENCY, DynamicOptimizedDependencyGraphImpl.class)
          .set(DynamicMWOConfiguration.DYNAMIC_PARTIAL, DynamicOptimizedPartialTimespans.class)
          .set(DynamicMWOConfiguration.START_TIME, startTime)
          .build();
      threads.add(new Thread(new Runnable() {
        @Override
        public void run() {
          final Injector injector = Tang.Factory.getTang().newInjector(conf);
          injector.bindVolatileInstance(TimeWindowOutputHandler.class, timeWindowOutputHandler);
          final DynamicMWO<I,V> mwo;
          try {
            mwo = injector.getInstance(DynamicMWO.class);
          } catch (InjectionException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
          }

          final BlockingQueue<I> queue = new LinkedBlockingQueue<I>();
          final AtomicBoolean closed = new AtomicBoolean(false);
          dynamicMWOList.add(mwo);
          queueList.add(queue);
          closedList.add(closed);
          countDownLatch.countDown();

          while (!closed.get()) {
            while (!queue.isEmpty()) {
              final I val;
              try {
                val = queue.take();
                mwo.execute(val);
              } catch (InterruptedException e) {
                e.printStackTrace();
                //throw new RuntimeException(e);
              }
            }
          }

          LOG.info("END OF thread " + index);
          endCountDown.countDown();
        }
      }));
    }

    for (final Thread thread : threads) {
      thread.start();
    }

    // Waiting all threads is running
    countDownLatch.await();
  }

  /**
   * Aggregates input into the current bucket.
   * @param val input
   */
  @Override
  public void execute(final I val) {
    if (val instanceof WindowTimeEvent) {
      for (final Queue<I> queue : queueList) {
        queue.add(val);
      }
    } else {
      // Select an MWO
      final int index = Math.abs(val.hashCode()) % numThreads;
      final Queue<I> queue = queueList.get(index);
      queue.add(val);
    }
  }

  @Override
  public void prepare(final OutputEmitter<TimescaleWindowOutput<V>> outputEmitter) {

  }

  @Override
  public void close() throws Exception {
    LOG.info("Closing " + this.getClass());
    for (final Thread thread : threads) {
      thread.interrupt();
    }
    for (final AtomicBoolean closed : closedList) {
      closed.set(true);
    }

    endCountDown.await();
  }

  @Override
  public void addWindow(final Timescale window, final long time) {

  }

  @Override
  public void removeWindow(final Timescale window, final long time) {

  }
}