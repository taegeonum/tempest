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
package edu.snu.tempest.operator.window.time.sts.impl;

import edu.snu.tempest.operator.window.aggregator.ComAndAscAggregator;
import edu.snu.tempest.operator.window.time.Timescale;
import edu.snu.tempest.operator.window.time.common.StaticComputationReuserImpl;
import edu.snu.tempest.operator.window.time.common.StaticSlicedWindowOperatorImpl;
import edu.snu.tempest.operator.window.time.mts.impl.SlicedWindowOperator;
import edu.snu.tempest.operator.window.time.mts.parameters.InitialStartTime;
import edu.snu.tempest.operator.window.time.sts.STSWindowOperator;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * STSWindowOperatorImpl receives single timescale
 * and produces outputs.
 * TODO: current sts operator just supports commutative/associative aggregation, need to support general aggregation.
 * @param <I> input
 * @param <V> output
 */
public final class STSWindowOperatorImpl<I, V> implements STSWindowOperator<I> {
  private static final Logger LOG = Logger.getLogger(STSWindowOperatorImpl.class.getName());

  /**
   * Is this window operator started or not.
   */
  private final AtomicBoolean started = new AtomicBoolean(false);

  /**
   * A single timescale scheduler for window operation.
   */
  private final STSOperatorScheduler scheduler;

  /**
   * A sliced window operator for partial aggregation.
   */
  private final SlicedWindowOperator<I> slicedWindow;

  /**
   * STSWindowOperatorImpl.
   * @param aggregator an aggregator for window aggregation
   * @param timescale a timescale
   * @param handler an mts output handler
   * @param startTime an initial start time of the operator
   */
  @Inject
  private STSWindowOperatorImpl(final ComAndAscAggregator<I, V> aggregator,
                               final Timescale timescale,
                               final STSOutputHandler<V> handler,
                               @Parameter(InitialStartTime.class) final long startTime) {
    final List<Timescale> timescales = new LinkedList<>();
    timescales.add(timescale);
    final StaticComputationReuserImpl<V> computationReuser =
        new StaticComputationReuserImpl<>(timescales, aggregator, startTime);
    this.slicedWindow = new StaticSlicedWindowOperatorImpl<>(aggregator,
        computationReuser, startTime);
    this.scheduler = new STSOperatorScheduler(slicedWindow,
        new STSOverlappingWindowOperator<>(timescale, computationReuser, handler, startTime));
  }

  /**
   * Start mts window operation.
   */
  @Override
  public void start() {
    if (started.compareAndSet(false, true)) {
      LOG.log(Level.INFO, "MTSOperator start");
      this.scheduler.start();
    }
  }

  /**
   * Aggregate input and produce partially aggregated outputs.
   * @param val input value
   */
  @Override
  public void execute(final I val) {
    LOG.log(Level.FINEST, "MTSOperator execute : ( " + val + ")");
    this.slicedWindow.execute(val);
  }

  @Override
  public void close() throws Exception {
    scheduler.close();
  }
}
