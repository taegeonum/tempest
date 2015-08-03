/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package edu.snu.tempest.operator.window.time.mts.impl;

import edu.snu.tempest.operator.common.Subscription;
import edu.snu.tempest.operator.window.aggregator.AssociativeAggregator;
import edu.snu.tempest.operator.window.time.Timescale;
import edu.snu.tempest.operator.window.time.mts.CachingPolicy;
import edu.snu.tempest.operator.window.time.mts.MTSWindowOperator;
import edu.snu.tempest.operator.window.time.mts.TimescaleSignalListener;
import edu.snu.tempest.operator.window.time.mts.parameters.StartTime;
import edu.snu.tempest.operator.window.time.mts.signal.MTSSignalReceiver;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * DynamicMTSOperatorImpl dynamically adds/removes timescales
 * and produces multi-timescale outputs.
 * TODO: current mts operator just supports commutative/associative aggregation, need to support general aggregation.
 * @param <I> input
 * @param <V> output
 */
public final class DynamicMTSOperatorImpl<I, V> implements MTSWindowOperator<I> {
  private static final Logger LOG = Logger.getLogger(DynamicMTSOperatorImpl.class.getName());
  /**
   * Is this window operator started or not.
   */
  private final AtomicBoolean started = new AtomicBoolean(false);

  /**
   * An output handler for multi-time scale window outputs.
   */
  private final MTSWindowOperator.MTSOutputHandler<V> outputHandler;

  /**
   * A mts scheduler for mts window operation.
   */
  private final MTSOperatorScheduler scheduler;

  /**
   * A computation reuser for creating window outputs.
   */
  private final DynamicComputationReuser<V> computationReuser;

  /**
   * A sliced window operator for mts partial aggregation.
   */
  private final DynamicSlicedWindowOperator<I, V> slicedWindow;

  /**
   * OverlappingWindowOperator subscriptions.
   */
  private final Map<Timescale, Subscription<Timescale>> subscriptions;

  /**
   * SignalReceiver for triggering timescale addition/deletion.
   */
  private final MTSSignalReceiver receiver;

  /**
   * DynamicMTSOperatorImpl.
   * @param aggregator an aggregator for window aggregation
   * @param timescales an initial timescales
   * @param handler an mts output handler
   * @param receiver a receiver for triggering timescale addition/deletion
   * @param cachingPolicy a cachingRatePolicy
   * @param startTime an initial start time of the operator
   */
  @Inject
  private DynamicMTSOperatorImpl(final AssociativeAggregator<I, V> aggregator,
                                 final List<Timescale> timescales,
                                 final MTSWindowOperator.MTSOutputHandler<V> handler,
                                 final MTSSignalReceiver receiver,
                                 final CachingPolicy cachingPolicy,
                                 @Parameter(StartTime.class) final long startTime) {
    this.outputHandler = handler;
    this.computationReuser = new DynamicComputationReuser<>(timescales,
        aggregator, cachingPolicy, startTime);
    this.subscriptions = new HashMap<>();
    this.receiver = receiver;
    this.receiver.addTimescaleSignalListener(new SignalListener());

    this.slicedWindow = new DynamicSlicedWindowOperator<>(aggregator, timescales,
        computationReuser, startTime);
    this.scheduler = new MTSOperatorScheduler(slicedWindow);

    for (final Timescale timescale : timescales) {
      final DefaultOverlappingWindowOperator<V> owo = new DefaultOverlappingWindowOperator<V>(
          timescale, computationReuser, outputHandler, startTime);
      final Subscription<Timescale> ss = scheduler.subscribe(owo);
      subscriptions.put(ss.getToken(), ss);
    }
  }

  /**
   * Start mts window operation.
   */
  @Override
  public void start() {
    if (started.compareAndSet(false, true)) {
      LOG.log(Level.INFO, DynamicMTSOperatorImpl.class.getName() + " start");
      this.scheduler.start();
      try {
        this.receiver.start();
      } catch (final Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Aggregate input and produce partially aggregated outputs.
   * @param val input value
   */
  @Override
  public void execute(final I val) {
    LOG.log(Level.FINEST, DynamicMTSOperatorImpl.class.getName() + " execute : ( " + val + ")");
    this.slicedWindow.execute(val);
  }


  @Override
  public void close() throws Exception {
    scheduler.close();
  }

  class SignalListener implements TimescaleSignalListener {
    /**
     * Add a timescale dynamically and produce outputs for this timescale.
     * @param ts timescale to be added.
     * @param startTime the time when timescale is added. This is used for synchronization.
     */
    @Override
    public synchronized void onTimescaleAddition(final Timescale ts, final long startTime) {
      LOG.log(Level.INFO, DynamicMTSOperatorImpl.class.getName() + " addTimescale: " + ts);
      //1. add timescale to SlicedWindowOperator
      DynamicMTSOperatorImpl.this.slicedWindow.addTimescale(ts, startTime);

      //2. add timescale to computationReuser.
      DynamicMTSOperatorImpl.this.computationReuser.addTimescale(ts, startTime);

      //3. add overlapping window operator
      final DefaultOverlappingWindowOperator<V> owo = new DefaultOverlappingWindowOperator<>(
          ts, computationReuser, outputHandler, startTime);
      final Subscription<Timescale> ss = DynamicMTSOperatorImpl.this.scheduler.subscribe(owo);
      subscriptions.put(ss.getToken(), ss);
    }

    /**
     * Remove a timescale dynamically.
     * @param ts timescale to be deleted.
     */
    @Override
    public synchronized void onTimescaleDeletion(final Timescale ts) {
      LOG.log(Level.INFO, DynamicMTSOperatorImpl.class.getName() + " removeTimescale: " + ts);
      final Subscription<Timescale> ss = subscriptions.get(ts);
      if (ss == null) {
        LOG.log(Level.WARNING, "Deletion error: Timescale " + ts + " not exists. ");
      } else {
        DynamicMTSOperatorImpl.this.slicedWindow.removeTimescale(ts);
        DynamicMTSOperatorImpl.this.computationReuser.removeTimescale(ts);
        ss.unsubscribe();
      }
    }
  }
}
