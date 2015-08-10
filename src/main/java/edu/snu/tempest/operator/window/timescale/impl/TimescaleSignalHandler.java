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

import edu.snu.tempest.operator.common.Subscription;
import edu.snu.tempest.operator.window.timescale.TimescaleWindowOutputHandler;
import edu.snu.tempest.operator.window.timescale.Timescale;
import edu.snu.tempest.operator.window.timescale.TimescaleParser;
import edu.snu.tempest.operator.window.timescale.parameter.StartTime;
import edu.snu.tempest.signal.window.timescale.TimescaleSignal;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Timescale signal handler for dynamic multi-time scale.
 * It handles dynamic timescale addition/deletion and changes the behavior of the mts operator.
 */
final class TimescaleSignalHandler<V> implements EventHandler<TimescaleSignal> {
  private static final Logger LOG = Logger.getLogger(TimescaleSignalHandler.class.getName());

  private final OverlappingWindowStage owoStage;
  private final ComputationReuser<V> computationReuser;
  private final TimescaleWindowOutputHandler<V> outputHandler;
  private final Map<Timescale, Subscription<OverlappingWindowOperator>> subscriptions;

  /**
   * Timescale signal handler for dynamic multi-time scale.
   * @param owoStage a stage for multiple overlapping window operators
   * @param computationReuser a computation reuser
   * @param outputHandler an output handler
   * @param tsParser timescale parser
   * @param startTime a start time
   */
  public TimescaleSignalHandler(
      final OverlappingWindowStage owoStage,
      final ComputationReuser<V> computationReuser,
      final TimescaleWindowOutputHandler<V> outputHandler,
      final TimescaleParser tsParser,
      @Parameter(StartTime.class) final long startTime) {
    this.owoStage = owoStage;
    this.computationReuser = computationReuser;
    this.outputHandler = outputHandler;
    this.subscriptions = new HashMap<>();
    // add overlapping window operators
    for (final Timescale timescale : tsParser.timescales) {
      final OverlappingWindowOperator owo = new DefaultOverlappingWindowOperator<V>(
          timescale, computationReuser, outputHandler, startTime);
      final Subscription<OverlappingWindowOperator> ss = this.owoStage.subscribe(owo);
      subscriptions.put(ss.getToken().getTimescale(), ss);
    }
  }

  @Override
  public void onNext(final TimescaleSignal ts) {
    final Timescale timescale = new Timescale(ts.windowSize, ts.interval);

    if (ts.type == TimescaleSignal.ADDITION) {
      LOG.log(Level.INFO, MTSOperatorImpl.class.getName() + " addTimescale: " + timescale);
      //1. add timescale to computationReuser.
      this.computationReuser.onTimescaleAddition(timescale, ts.startTime);

      //2. add overlapping window operator
      final OverlappingWindowOperator owo = new DefaultOverlappingWindowOperator<>(
          timescale, computationReuser, outputHandler, ts.startTime);
      final Subscription<OverlappingWindowOperator> ss = this.owoStage.subscribe(owo);
      subscriptions.put(ss.getToken().getTimescale(), ss);
    } else {
      LOG.log(Level.INFO, MTSOperatorImpl.class.getName() + " removeTimescale: " + timescale);
      final Subscription<OverlappingWindowOperator> ss = subscriptions.get(timescale);
      if (ss == null) {
        LOG.log(Level.WARNING, "Deletion error: Timescale " + ts + " not exists. ");
      } else {
        this.computationReuser.onTimescaleDeletion(timescale, ts.startTime);
        ss.unsubscribe();
      }
    }
  }
}