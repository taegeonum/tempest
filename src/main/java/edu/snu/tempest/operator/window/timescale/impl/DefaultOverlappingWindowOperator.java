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

import edu.snu.tempest.operator.OutputEmitter;
import edu.snu.tempest.operator.window.timescale.Timescale;
import edu.snu.tempest.operator.window.timescale.TimescaleWindowOutput;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This operator executes final aggregation every interval.
 * It receives current time and triggers final aggregation.
 */
final class DefaultOverlappingWindowOperator<I> implements OverlappingWindowOperator<I> {
  private static final Logger LOG = Logger.getLogger(DefaultOverlappingWindowOperator.class.getName());

  /**
   * A timescale related to this overlapping window operator.
   */
  private final Timescale timescale;

  /**
   * A computation reuser for creating window outputs.
   */
  private final ComputationReuser<I> computationReuser;

  /**
   * An output handler for window output.
   */
  private OutputEmitter<TimescaleWindowOutput<I>> emitter;

  /**
   * An initial start time of the window operator.
   */
  private final long startTime;

  /**
   * Default overlapping window operator.
   * @param timescale a timescale
   * @param computationReuser a computation reuser for final aggregation
   * @param startTime an initial start time
   */
  public DefaultOverlappingWindowOperator(final Timescale timescale,
                                          final ComputationReuser<I> computationReuser,
                                          final long startTime) {
    this.timescale = timescale;
    this.computationReuser = computationReuser;
    this.startTime = startTime;
  }

  /**
   * Return the timescale.
   * @return the timescale.
   */
  public Timescale getTimescale() {
    return timescale;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("[OLO: ");
    sb.append(timescale);
    sb.append("]");
    return sb.toString();
  }

  /**
   * Executes final aggregation every interval.
   * @param currTime
   */
  @Override
  public void execute(final Long currTime) {
    LOG.log(Level.FINE, "OverlappingWindowOperator triggered: " + currTime + ", timescale: " + timescale
        + ", " + (currTime - startTime) % timescale.intervalSize);
    if (((currTime - startTime) % timescale.intervalSize) == 0) {
      LOG.log(Level.FINE, "OverlappingWindowOperator final aggregation: " + currTime + ", timescale: " + timescale);
      final long endTime = currTime;
      final long start = Math.max(startTime, endTime - timescale.windowSize);
      try {
        final boolean fullyProcessed = this.startTime <= endTime - timescale.windowSize;
        final I finalResult = computationReuser.finalAggregate(start, currTime, timescale);
        // send the result to output emitter
        emitter.emit(new TimescaleWindowOutput<>(
            timescale, finalResult, endTime - timescale.windowSize, currTime, fullyProcessed));
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Saves output information before executing final aggregation.
   * This function should be called before .onNext(time)
   * @param currTime current time
   */
  public void saveOutputInformation(final Long currTime) {
    if (((currTime - startTime) % timescale.intervalSize) == 0) {
      final long endTime = currTime;
      final long start = endTime - timescale.windowSize;
      computationReuser.saveOutputInformation(start, currTime, timescale);
    }
  }

  @Override
  public void prepare(final OutputEmitter<TimescaleWindowOutput<I>> outputEmitter) {
    emitter = outputEmitter;
  }
}