package org.edu.snu.tempest.operator.impl;

import org.edu.snu.tempest.Timescale;
import org.edu.snu.tempest.operator.MTSOperator.OutputHandler;
import org.edu.snu.tempest.operator.OverlappingWindowOperator;
import org.edu.snu.tempest.operator.WindowOutput;
import org.edu.snu.tempest.operator.relationcube.RelationCube;

import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.logging.Level;

public final class DefaultOverlappingWindowOperatorImpl<V> implements OverlappingWindowOperator<V> {
  private static final Logger LOG = Logger.getLogger(DefaultOverlappingWindowOperatorImpl.class.getName());

  private final Timescale timescale;
  private final RelationCube<V> relationCube;
  private final OutputHandler<V> outputHandler;
  private final long startTime;

  public DefaultOverlappingWindowOperatorImpl(final Timescale timescale,
                                              final RelationCube<V> relationCube,
                                              final OutputHandler<V> outputHandler,
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
      long aggStartTime = System.nanoTime();
      final long endTime = currTime;
      final long start = endTime - timescale.windowSize;
      try {
        final V finalResult = relationCube.finalAggregate(start, currTime, timescale);
        // send the result
        long etime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - aggStartTime);
        outputHandler.onNext(new WindowOutput<>(timescale, finalResult, start, currTime, etime));
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
