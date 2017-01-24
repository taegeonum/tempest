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
package edu.snu.tempest.operator.window.timescale.impl;

import edu.snu.tempest.operator.common.DefaultSubscription;
import edu.snu.tempest.operator.common.Subscription;
import edu.snu.tempest.operator.common.SubscriptionHandler;
import atc.operator.window.timescale.parameter.NumThreads;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EStage;
import org.apache.reef.wake.WakeParameters;
import org.apache.reef.wake.impl.StageManager;

import javax.inject.Inject;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A stage that triggers overlapping window operators, which execute final aggregation.
 */
final class FinalAggregatorManager<V> implements EStage<Long> {
  private static final Logger LOG = Logger.getLogger(FinalAggregatorManager.class.getCanonicalName());

  /**
   * Is this stage closed or not.
   */
  private final AtomicBoolean closed = new AtomicBoolean(false);

  /**
   * An executor for overlapping window operators.
   */
  private final ExecutorService executor;

  /**
   * A priority queue ordering event handlers.
   */
  private final PriorityQueue<FinalAggregator<V>> handlers;

  /**
   * A read/write lock for queue.
   */
  private final ReadWriteLock lock = new ReentrantReadWriteLock();

  /**
   * Shutdown timeout.
   */
  private final long shutdownTimeout = WakeParameters.EXECUTOR_SHUTDOWN_TIMEOUT;

  /**
   * Subscription handler.
   */
  private final OWOSubscriptionHandler subscriptionHandler;

  /**
   * Overlapping window stage for doing final aggregation.
   */
  @Inject
  private FinalAggregatorManager(@Parameter(NumThreads.class) final int numThreads) {
    this.handlers = new PriorityQueue<>(10, new OWOComparator());
    this.executor = Executors.newFixedThreadPool(numThreads);
    this.subscriptionHandler = new OWOSubscriptionHandler();
    StageManager.instance().register(this);
  }

  @Override
  public void close() throws Exception {
    if (closed.compareAndSet(false, true)) {
      executor.shutdown();
      if (!executor.awaitTermination(shutdownTimeout, TimeUnit.MILLISECONDS)) {
        LOG.log(Level.WARNING, "Executor did not terminate in " + shutdownTimeout + "ms.");
        final List<Runnable> droppedRunnables = executor.shutdownNow();
        LOG.log(Level.WARNING, "Executor dropped " + droppedRunnables.size() + " tasks.");
      }
    }
  }

  @Override
  public void onNext(final Long time) {
    lock.readLock().lock();
    // save output information before final aggregation.
    for (final FinalAggregator owo : handlers) {
      owo.saveOutputInformation(time);
    }
    // do final aggregation
    for (final FinalAggregator<V> owo : handlers) {
      executor.submit(new Runnable() {
        @Override
        public void run() {
          owo.execute(time);
        }
      });
    }
    lock.readLock().unlock();
  }


  /**
   * Subscribes an overlapping window operator.
   * @param handler an overlapping window operator
   * @return a subscription for unsubscribe
   */
  public Subscription<FinalAggregator<V>> subscribe(final FinalAggregator<V> handler) {
    LOG.log(Level.FINE, "Subscribe " + handler);
    lock.writeLock().lock();
    handlers.add(handler);
    lock.writeLock().unlock();
    return new DefaultSubscription<>(subscriptionHandler, handler);
  }

  /**
   * Subscription handler for unsubscribe overlaping window operators.
   */
  class OWOSubscriptionHandler implements SubscriptionHandler<FinalAggregator<V>> {
    /**
     * Unsubscribe the subscription.
     * @param subscription a subscription
     */
    @Override
    public void unsubscribe(final Subscription<FinalAggregator<V>> subscription) {
      LOG.log(Level.FINE, "Unsubscribe " + subscription.getToken());
      FinalAggregatorManager.this.lock.writeLock().lock();
      FinalAggregatorManager.this.handlers.remove(subscription.getToken());
      FinalAggregatorManager.this.lock.writeLock().unlock();
    }
  }
}