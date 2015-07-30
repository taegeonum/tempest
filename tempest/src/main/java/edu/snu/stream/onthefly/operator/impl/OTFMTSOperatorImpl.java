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
package edu.snu.stream.onthefly.operator.impl;

import edu.snu.tempest.operators.Timescale;
import edu.snu.tempest.operators.common.Aggregator;
import edu.snu.tempest.operators.common.MTSOperatorScheduler;
import edu.snu.tempest.operators.common.OverlappingWindowOperator;
import edu.snu.tempest.operators.common.Subscription;
import edu.snu.tempest.operators.common.impl.DefaultMTSOperatorSchedulerImpl;
import edu.snu.tempest.operators.common.impl.DefaultOverlappingWindowOperatorImpl;
import edu.snu.tempest.operators.dynamicmts.DynamicMTSOperator;
import edu.snu.tempest.operators.dynamicmts.DynamicSlicedWindowOperator;
import edu.snu.tempest.operators.dynamicmts.impl.DynamicMTSOperatorImpl;
import edu.snu.tempest.operators.dynamicmts.impl.DynamicSlicedWindowOperatorImpl;
import edu.snu.tempest.operators.dynamicmts.signal.MTSSignalReceiver;

import javax.inject.Inject;
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
 */
public final class OTFMTSOperatorImpl<I, V> implements DynamicMTSOperator<I> {
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
   * A relationCube for saving outputs.
   */
  private final OTFRelationCubeImpl<V> relationCube;

  /**
   * A sliced window operator for partial aggregation.
   */
  private final DynamicSlicedWindowOperator<I> slicedWindow;

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
  @Inject
  public OTFMTSOperatorImpl(final Aggregator<I, V> aggregator,
                                final List<Timescale> timescales,
                                final MTSOutputHandler<V> handler,
                                final MTSSignalReceiver receiver,
                                final Long startTime) {
    this.outputHandler = handler;
    this.relationCube = new OTFRelationCubeImpl<>(timescales, aggregator, startTime);
    this.subscriptions = new HashMap<>();
    this.receiver = receiver;
    this.receiver.addTimescaleSignalListener(this);

    this.slicedWindow = new DynamicSlicedWindowOperatorImpl<>(aggregator, timescales,
        relationCube, startTime);
    this.scheduler = new DefaultMTSOperatorSchedulerImpl(slicedWindow);

    for (final Timescale timescale : timescales) {
      final OverlappingWindowOperator<V> owo = new DefaultOverlappingWindowOperatorImpl<V>(
          timescale, relationCube, outputHandler, startTime);
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
      LOG.log(Level.INFO, "MTSOperator start");
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
    LOG.log(Level.FINEST, "OTFMTSOperatorImpl execute : ( " + val + ")");
    this.slicedWindow.execute(val);
  }

  /**
   * Add a timescale dynamically and produce outputs for this timescale.
   * @param ts timescale to be added.
   * @param startTime the time when timescale is added. This is used for synchronization.
   */
  @Override
  public synchronized void onTimescaleAddition(final Timescale ts, final long startTime) {
    //1. add timescale to SlicedWindowOperator
    this.slicedWindow.onTimescaleAddition(ts, startTime);

    //2. add timescale to RelationCube.
    this.relationCube.onTimescaleAddition(ts, startTime);

    //3. add overlapping window operator
    LOG.log(Level.INFO, "MTSOperator addTimescale: " + ts);
    final OverlappingWindowOperator<V> owo = new DefaultOverlappingWindowOperatorImpl<>(
        ts, relationCube, outputHandler, startTime);
    final Subscription<Timescale> ss = this.scheduler.subscribe(owo);
    subscriptions.put(ss.getToken(), ss);
  }

  /**
   * Remove a timescale dynamically.
   * @param ts timescale to be deleted.
   */
  @Override
  public synchronized void onTimescaleDeletion(final Timescale ts) {
    LOG.log(Level.INFO, "OTFMTSOperatorImpl removeTimescale: " + ts);
    final Subscription<Timescale> ss = subscriptions.get(ts);
    if (ss == null) {
      LOG.log(Level.WARNING, "Deletion error: Timescale " + ts + " not exists. ");
    } else {
      ss.unsubscribe();
      this.relationCube.onTimescaleDeletion(ts);
      this.slicedWindow.onTimescaleDeletion(ts);
    }
  }

  @Override
  public void close() throws Exception {
    scheduler.close();
  }
}
