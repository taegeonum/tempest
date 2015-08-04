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
package edu.snu.tempest.operator.window.time.impl;

import edu.snu.tempest.operator.window.WindowOperator;
import edu.snu.tempest.operator.window.time.TimeWindowOutputHandler;
import edu.snu.tempest.operator.window.time.Timescale;
import edu.snu.tempest.operator.window.time.TimescaleParser;
import edu.snu.tempest.operator.window.time.parameter.StartTime;
import edu.snu.tempest.operator.window.time.signal.MTSSignalReceiver;
import edu.snu.tempest.operator.window.time.signal.TimescaleSignalListener;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Multi-time scale sliding window operator.
 * TODO: current mts operator just supports commutative/associative aggregation, need to support general aggregation.
 * @param <I> input
 * @param <V> output
 */
public final class MTSOperatorImpl<I, V> implements WindowOperator<I> {
  private static final Logger LOG = Logger.getLogger(MTSOperatorImpl.class.getName());

  /**
   * A sliced window operator for partial aggregation.
   */
  private final SlicedWindowOperator<I> slicedWindowOperator;

  /**
   * This constructor creates dynamic timescale window operator.
   */
  @Inject
  private MTSOperatorImpl(
      final SlicedWindowStage slicedWindowStage,
      final OverlappingWindowStage owoStage,
      final ComputationReuser<V> computationReuser,
      final TimeWindowOutputHandler<V> outputHandler,
      final TimescaleParser tsParser,
      final SlicedWindowOperator<I> slicedWindowOperator,
      @Parameter(StartTime.class) final long startTime,
      final TimescaleSignalListener signalListener,
      final MTSSignalReceiver receiver) throws Exception {
    receiver.addTimescaleSignalListener(signalListener);
    receiver.start();
    this.slicedWindowOperator = slicedWindowOperator;
  }

  /**
   * This constructor creates static timescale window operator.
   */
  @Inject
  private MTSOperatorImpl(
      final SlicedWindowStage slicedWindowStage,
      final OverlappingWindowStage owoStage,
      final ComputationReuser<V> computationReuser,
      final TimeWindowOutputHandler<V> outputHandler,
      final TimescaleParser tsParser,
      final SlicedWindowOperator<I> slicedWindowOperator,
      @Parameter(StartTime.class) final long startTime) throws Exception {
    this.slicedWindowOperator = slicedWindowOperator;
    // add overlapping window operators
    for (final Timescale timescale : tsParser.timescales) {
      final OverlappingWindowOperator owo = new DefaultOverlappingWindowOperator<V>(
          timescale, computationReuser, outputHandler, startTime);
      owoStage.subscribe(owo);
    }
  }

  /**
   * Aggregate input and produce partially aggregated outputs.
   * @param val input value
   */
  @Override
  public void execute(final I val) {
    LOG.log(Level.FINEST, MTSOperatorImpl.class.getName() + " execute : ( " + val + ")");
    this.slicedWindowOperator.execute(val);
  }
}
