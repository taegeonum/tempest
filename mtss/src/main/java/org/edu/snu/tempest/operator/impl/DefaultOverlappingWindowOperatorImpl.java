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
  private final LogicalTime startTime;

  public DefaultOverlappingWindowOperatorImpl(final Timescale timescale,
                                              final RelationCube<V> relationCube,
                                              final OutputHandler<V> outputHandler,
                                              final LogicalTime startTime) {
    this.timescale = timescale;
    this.relationCube = relationCube;
    this.outputHandler = outputHandler;
    this.startTime = startTime;
  }
  
  @Override
  public synchronized void onNext(LogicalTime time) {
    LOG.log(Level.FINE, "OverlappingWindowOperator triggered: " + time + ", timescale: " + timescale);
    if (((time.logicalTime - startTime.logicalTime) % timescale.intervalSize) == 0) {
      LOG.log(Level.FINE, "OverlappingWindowOperator final aggregation: " + time + ", timescale: " + timescale);
      long aggStartTime = System.nanoTime();
      final long endTime = time.logicalTime;
      final long start = endTime - timescale.windowSize;
      final V finalResult = relationCube.finalAggregate(start, time.logicalTime, timescale);
      // send the result
      long etime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - aggStartTime);
      outputHandler.onNext(new WindowOutput<>(timescale, finalResult, start, time.logicalTime, etime));
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
