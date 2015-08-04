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

import edu.snu.tempest.operator.common.Subscription;
import edu.snu.tempest.operator.window.time.TimeWindowOutputHandler;
import edu.snu.tempest.operator.window.time.Timescale;
import edu.snu.tempest.operator.window.time.TimescaleParser;
import edu.snu.tempest.operator.window.time.parameter.StartTime;
import edu.snu.tempest.operator.window.time.signal.TimescaleSignalListener;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Timescale signal listener for dynamic multi-time scale.
 * It handles dynamic timescale addition/deletion and changes the behavior of the mts operator.
 */
public final class DefaultTSSignalListener<V> implements TimescaleSignalListener {
  private static final Logger LOG = Logger.getLogger(DefaultTSSignalListener.class.getName());

  private final NextSliceTimeProvider sliceTimeProvider;
  private final OverlappingWindowStage owoStage;
  private final ComputationReuser<V> computationReuser;
  private final TimeWindowOutputHandler<V> outputHandler;
  private final Map<Timescale, Subscription<Timescale>> subscriptions;

  /**
   * Timescale signal listener for dynamic multi-time scale.
   * @param sliceTimeProvider a slice time prodiver
   * @param owoStage a stage for multiple overlapping window operators
   * @param computationReuser a computation reuser
   * @param outputHandler an output handler
   * @param tsParser timescale parser
   * @param startTime a start time
   */
  @Inject
  private DefaultTSSignalListener(
      final NextSliceTimeProvider sliceTimeProvider,
      final OverlappingWindowStage owoStage,
      final ComputationReuser<V> computationReuser,
      final TimeWindowOutputHandler<V> outputHandler,
      final TimescaleParser tsParser,
      @Parameter(StartTime.class) final long startTime) {
    this.sliceTimeProvider = sliceTimeProvider;
    this.owoStage = owoStage;
    this.computationReuser = computationReuser;
    this.outputHandler = outputHandler;
    this.subscriptions = new HashMap<>();
    // add overlapping window operators
    for (final Timescale timescale : tsParser.timescales) {
      final OverlappingWindowOperator owo = new DefaultOverlappingWindowOperator<V>(
          timescale, computationReuser, outputHandler, startTime);
      final Subscription<Timescale> ss = this.owoStage.subscribe(owo);
      subscriptions.put(ss.getToken(), ss);
    }
  }

  /**
   * Add a timescale dynamically and produce outputs for this timescale.
   * @param ts timescale to be added.
   * @param addTime the time when timescale is added.
   */
  @Override
  public synchronized void onTimescaleAddition(final Timescale ts, final long addTime) {
    LOG.log(Level.INFO, MTSOperatorImpl.class.getName() + " addTimescale: " + ts);
    //1. add timescale to slice time provider
    this.sliceTimeProvider.onTimescaleAddition(ts, addTime);

    //2. add timescale to computationReuser.
    this.computationReuser.onTimescaleAddition(ts, addTime);

    //3. add overlapping window operator
    final OverlappingWindowOperator owo = new DefaultOverlappingWindowOperator<>(
        ts, computationReuser, outputHandler, addTime);
    final Subscription<Timescale> ss = this.owoStage.subscribe(owo);
    subscriptions.put(ss.getToken(), ss);
  }

  /**
   * Remove a timescale dynamically.
   * @param ts timescale to be deleted.
   * @param deleteTime the time when timescale is deleted.
   */
  @Override
  public synchronized void onTimescaleDeletion(final Timescale ts, final long deleteTime) {
    LOG.log(Level.INFO, MTSOperatorImpl.class.getName() + " removeTimescale: " + ts);
    final Subscription<Timescale> ss = subscriptions.get(ts);
    if (ss == null) {
      LOG.log(Level.WARNING, "Deletion error: Timescale " + ts + " not exists. ");
    } else {
      this.sliceTimeProvider.onTimescaleDeletion(ts, deleteTime);
      this.computationReuser.onTimescaleDeletion(ts, deleteTime);
      ss.unsubscribe();
    }
  }
}