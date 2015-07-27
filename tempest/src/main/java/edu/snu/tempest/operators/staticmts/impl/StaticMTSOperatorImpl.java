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
package edu.snu.tempest.operators.staticmts.impl;

import edu.snu.tempest.operators.Timescale;
import edu.snu.tempest.operators.common.Aggregator;
import edu.snu.tempest.operators.common.impl.DefaultOverlappingWindowOperatorImpl;
import edu.snu.tempest.operators.staticmts.MTSOperator;
import org.apache.reef.tang.annotations.Parameter;
import edu.snu.tempest.operators.common.MTSOperatorScheduler;
import edu.snu.tempest.operators.common.impl.DefaultMTSOperatorSchedulerImpl;
import edu.snu.tempest.operators.parameters.InitialStartTime;
import edu.snu.tempest.operators.staticmts.SlicedWindowOperator;
import edu.snu.tempest.operators.staticmts.StaticRelationGraph;

import javax.inject.Inject;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * StaticMTSOperatorImpl receives a static list of timescales at construction time
 * and produces multi-timescale outputs.
 * @param <I> input
 * @param <V> output
 */
public final class StaticMTSOperatorImpl<I, V> implements MTSOperator<I> {
  private static final Logger LOG = Logger.getLogger(StaticMTSOperatorImpl.class.getName());

  /**
   * Is this window operator started or not.
   */
  private final AtomicBoolean started = new AtomicBoolean(false);

  /**
   * A mts scheduler for mts window operation.
   */
  private final MTSOperatorScheduler mtsScheduler;

  /**
   * A sliced window operator for mts partial aggregation.
   */
  private final SlicedWindowOperator<I> slicedWindow;

  /**
   * StaticMTSOperatorImpl.
   * @param aggregator an aggregator for window aggregation
   * @param timescales the list of static timescales
   * @param handler an mts output handler
   * @param startTime an initial start time of the operator
   */
  @Inject
  public StaticMTSOperatorImpl(final Aggregator<I, V> aggregator,
                               final List<Timescale> timescales,
                               final MTSOutputHandler<V> handler,
                               @Parameter(InitialStartTime.class) final long startTime) {
    final StaticRelationGraph<V> relationCube = new StaticRelationGraphImpl<>(timescales, aggregator, startTime);
    this.slicedWindow = new StaticSlicedWindowOperatorImpl<>(aggregator,
        relationCube, startTime);
    this.mtsScheduler = new DefaultMTSOperatorSchedulerImpl(slicedWindow);
    for (final Timescale ts : timescales) {
      final DefaultOverlappingWindowOperatorImpl<V> owo = new DefaultOverlappingWindowOperatorImpl<>(
          ts, relationCube, handler, startTime);
      mtsScheduler.subscribe(owo);
    }
  }

  /**
   * Start mts window operation.
   */
  @Override
  public void start() {
    if (started.compareAndSet(false, true)) {
      LOG.log(Level.INFO, "MTSOperator start");
      this.mtsScheduler.start();
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
    mtsScheduler.close();
  }
}
