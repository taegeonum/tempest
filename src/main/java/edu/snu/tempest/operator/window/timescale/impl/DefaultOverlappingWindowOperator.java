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

import edu.snu.tempest.operator.window.timescale.TimescaleWindowOutput;
import edu.snu.tempest.operator.window.timescale.TimescaleWindowOutputHandler;
import edu.snu.tempest.operator.window.timescale.Timescale;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * It triggers final aggregation every interval.
 */
final class DefaultOverlappingWindowOperator<V> implements OverlappingWindowOperator {
  private static final Logger LOG = Logger.getLogger(DefaultOverlappingWindowOperator.class.getName());

  /**
   * A timescale related to this overlapping window operator.
   */
  private final Timescale timescale;

  /**
   * A computation reuser for creating window outputs.
   */
  private final ComputationReuser<V> computationReuser;

  /**
   * An output handler for window output.
   */
  private final TimescaleWindowOutputHandler<V> outputHandler;

  /**
   * An initial start time of the window operator.
   */
  private final long startTime;

  /**
   * Default overlapping window operator.
   * @param timescale a timescale
   * @param computationReuser a computation reuser for final aggregation
   * @param outputHandler an output handler
   * @param startTime an initial start time
   */
  public DefaultOverlappingWindowOperator(final Timescale timescale,
                                          final ComputationReuser<V> computationReuser,
                                          final TimescaleWindowOutputHandler<V> outputHandler,
                                          final long startTime) {
    this.timescale = timescale;
    this.computationReuser = computationReuser;
    this.outputHandler = outputHandler;
    this.startTime = startTime;
  }

  /**
   * If elapsed time is multiple of the interval
   * then this operator executes final aggregation by using a computation reuser.
   * @param currTime current time
   */
  public synchronized void onNext(final Long currTime) {
    LOG.log(Level.FINE, "OverlappingWindowOperator triggered: " + currTime + ", timescale: " + timescale
        + ", " + (currTime - startTime) % timescale.intervalSize);
    if (((currTime - startTime) % timescale.intervalSize) == 0) {
      LOG.log(Level.FINE, "OverlappingWindowOperator final aggregation: " + currTime + ", timescale: " + timescale);
      final long endTime = currTime;
      final long start = endTime - timescale.windowSize;
      try {
        final boolean fullyProcessed = this.startTime <= start;
        final V finalResult = computationReuser.finalAggregate(start, currTime, timescale);
        // send the result
        outputHandler.onNext(new TimescaleWindowOutput<>(timescale, finalResult, start, currTime, fullyProcessed));
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    }
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
}
