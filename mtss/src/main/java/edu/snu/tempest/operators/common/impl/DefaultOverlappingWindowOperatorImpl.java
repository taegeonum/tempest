package edu.snu.tempest.operators.common.impl;

import edu.snu.tempest.operators.Timescale;
import edu.snu.tempest.operators.common.MTSWindowOutput;
import edu.snu.tempest.operators.common.OverlappingWindowOperator;
import edu.snu.tempest.operators.common.RelationCube;
import edu.snu.tempest.operators.staticmts.MTSOperator;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * It triggers final aggregation every its interval.
 */
public final class DefaultOverlappingWindowOperatorImpl<V> implements OverlappingWindowOperator<V> {
  private static final Logger LOG = Logger.getLogger(DefaultOverlappingWindowOperatorImpl.class.getName());

  /**
   * A timescale related to this overlapping window operator.
   */
  private final Timescale timescale;

  /**
   * A relation cube for calculating final aggregation.
   */
  private final RelationCube<V> relationCube;

  /**
   * An output handler for mts window output.
   */
  private final MTSOperator.MTSOutputHandler<V> outputHandler;

  /**
   * An initial start time of the window operator.
   */
  private final long startTime;

  /**
   * Default overlapping window operator.
   * @param timescale a timescale
   * @param relationCube a relation cube
   * @param outputHandler an output handler
   * @param startTime an initial start time
   */
  public DefaultOverlappingWindowOperatorImpl(final Timescale timescale,
                                              final RelationCube<V> relationCube,
                                              final MTSOperator.MTSOutputHandler<V> outputHandler,
                                              final long startTime) {
    this.timescale = timescale;
    this.relationCube = relationCube;
    this.outputHandler = outputHandler;
    this.startTime = startTime;
  }

  /**
   * If elapsed time is multiple of the interval
   * then this operator executes final aggregation by using a relation cube.
   * @param currTime current time
   */
  @Override
  public synchronized void onNext(final Long currTime) {
    LOG.log(Level.FINE, "OverlappingWindowOperator triggered: " + currTime + ", timescale: " + timescale
        + ", " + (currTime - startTime) % timescale.intervalSize);
    if (((currTime - startTime) % timescale.intervalSize) == 0) {
      LOG.log(Level.FINE, "OverlappingWindowOperator final aggregation: " + currTime + ", timescale: " + timescale);
      final long endTime = currTime;
      final long start = endTime - timescale.windowSize;
      try {
        final boolean fullyProcessed = this.startTime <= start;
        final V finalResult = relationCube.finalAggregate(start, currTime, timescale);
        // send the result
        outputHandler.onNext(new MTSWindowOutput<>(timescale, finalResult, start, currTime, fullyProcessed));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Return the timescale.
   * @return the timescale.
   */
  @Override
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
