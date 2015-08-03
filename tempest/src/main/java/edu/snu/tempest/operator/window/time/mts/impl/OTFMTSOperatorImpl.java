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
import edu.snu.tempest.operator.window.time.mts.MTSWindowOperator;
import edu.snu.tempest.operator.window.time.mts.TimescaleSignalListener;
import edu.snu.tempest.operator.window.time.mts.parameters.StartTime;
import edu.snu.tempest.operator.window.time.mts.signal.MTSSignalReceiver;
import org.apache.reef.tang.annotations.Parameter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A mts operator for multi-time scale sliding window operator.
 * Reference: S. Krishnamurthy, C. Wu, and M. Franklin. On-the-fly sharing
 * for streamed aggregation. In ACM SIGMOD, 2006
 * It reuses partial aggregation, but does not reuse final aggregation.
 */
public final class OTFMTSOperatorImpl<I, V> implements MTSWindowOperator<I> {
  private static final Logger LOG = Logger.getLogger(DynamicMTSOperatorImpl.class.getName());

  /**
   * Is this window operator started or not.
   */
  private final AtomicBoolean started = new AtomicBoolean(false);

  /**
   * An output handler for multi-time scale window outputs.
   */
  private final MTSOutputHandler<V> outputHandler;

  /**
   * A mts scheduler for mts window operation.
   */
  private final MTSOperatorScheduler scheduler;

  /**
   * An computationReuser for creating window outputs.
   */
  private final OTFComputationReuser<V> computationReuser;

  /**
   * A sliced window operator for partial aggregation.
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
   * "On-the-fly sharing for streamed aggregation"'s operator
   * for multi-time scale sliding window operator.
   * @param aggregator an aggregator for window aggregation.
   * @param timescales an initial timescales for multi-timescale window operation.
   * @param handler an output handler for receiving multi-timescale window outputs.
   * @param receiver a receiver for triggering timescale addition/deletion.
   * @param startTime an initial start time of the operator.
   */
  public OTFMTSOperatorImpl(final AssociativeAggregator<I, V> aggregator,
                            final List<Timescale> timescales,
                            final MTSOutputHandler<V> handler,
                            final MTSSignalReceiver receiver,
                            @Parameter(StartTime.class) final long startTime) {
    this.outputHandler = handler;
    this.computationReuser = new OTFComputationReuser<>(timescales, aggregator, startTime);
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
   * Start window operation.
   */
  @Override
  public void start() {
    if (started.compareAndSet(false, true)) {
      LOG.log(Level.INFO, OTFMTSOperatorImpl.class.getName() + " start");
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
    LOG.log(Level.FINEST, OTFMTSOperatorImpl.class.getName() + " execute : ( " + val + ")");
    this.slicedWindow.execute(val);
  }

  @Override
  public void close() throws Exception {
    this.scheduler.close();
  }

  class SignalListener implements TimescaleSignalListener {
    /**
     * Add a timescale dynamically and produce outputs for this timescale.
     * @param ts timescale to be added.
     * @param startTime the time when timescale is added. This is used for synchronization.
     */
    @Override
    public synchronized void onTimescaleAddition(final Timescale ts, final long startTime) {
      LOG.log(Level.INFO, OTFMTSOperatorImpl.class.getName() + " addTimescale: " + ts);
      //1. add timescale to SlicedWindowOperator
      OTFMTSOperatorImpl.this.slicedWindow.addTimescale(ts, startTime);

      //2. add timescale to computationReuser.
      OTFMTSOperatorImpl.this.computationReuser.addTimescale(ts, startTime);

      //3. add overlapping window operator
      final DefaultOverlappingWindowOperator<V> owo = new DefaultOverlappingWindowOperator<>(
          ts, computationReuser, outputHandler, startTime);
      final Subscription<Timescale> ss = OTFMTSOperatorImpl.this.scheduler.subscribe(owo);
      subscriptions.put(ss.getToken(), ss);
    }

    /**
     * Remove a timescale dynamically.
     * @param ts timescale to be deleted.
     */
    @Override
    public synchronized void onTimescaleDeletion(final Timescale ts) {
      LOG.log(Level.INFO, OTFMTSOperatorImpl.class.getName() + " removeTimescale: " + ts);
      final Subscription<Timescale> ss = subscriptions.get(ts);
      if (ss == null) {
        LOG.log(Level.WARNING, "Deletion error: Timescale " + ts + " not exists. ");
      } else {
        OTFMTSOperatorImpl.this.slicedWindow.removeTimescale(ts);
        OTFMTSOperatorImpl.this.computationReuser.removeTimescale(ts);
        ss.unsubscribe();
      }
    }
  }
}
