package org.edu.snu.tempest.operators.common.impl;

import org.edu.snu.tempest.operators.Timescale;
import org.edu.snu.tempest.operators.common.OverlappingWindowOperator;
import org.edu.snu.tempest.operators.common.RelationCube;
import org.edu.snu.tempest.operators.common.WindowOutput;
import org.edu.snu.tempest.operators.staticmts.MTSOperator;

import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * It triggers final aggregation every its interval.
 */
public final class DefaultOverlappingWindowOperatorImpl<V> implements OverlappingWindowOperator<V> {
  private static final Logger LOG = Logger.getLogger(DefaultOverlappingWindowOperatorImpl.class.getName());

  private final Timescale timescale;
  private final RelationCube<V> relationCube;
  private final MTSOperator.OutputHandler<V> outputHandler;
  private final long startTime;

  public DefaultOverlappingWindowOperatorImpl(final Timescale timescale,
                                              final RelationCube<V> relationCube,
                                              final MTSOperator.OutputHandler<V> outputHandler,
                                              final long startTime) {
    this.timescale = timescale;
    this.relationCube = relationCube;
    this.outputHandler = outputHandler;
    this.startTime = startTime;
  }
  
  @Override
  public synchronized void onNext(final Long currTime) {
    LOG.log(Level.FINE, "OverlappingWindowOperator triggered: " + currTime + ", timescale: " + timescale
        + ", " + (currTime - startTime) % timescale.intervalSize);
    if (((currTime - startTime) % timescale.intervalSize) == 0) {
      LOG.log(Level.FINE, "OverlappingWindowOperator final aggregation: " + currTime + ", timescale: " + timescale);
      final long aggStartTime = System.nanoTime();
      final long endTime = currTime;
      final long start = endTime - timescale.windowSize;
      try {
        final boolean fullyProcessed = this.startTime <= start;
        final V finalResult = relationCube.finalAggregate(start, currTime, timescale);
        // send the result
        final long etime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - aggStartTime);
        outputHandler.onNext(new WindowOutput<>(timescale, finalResult, start, currTime, etime, fullyProcessed));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public Timescale getTimescale() {
    return timescale;
  }
  
  @Override
  public String toString() {
    return "[OLO: " + timescale + "]";
  }

}
