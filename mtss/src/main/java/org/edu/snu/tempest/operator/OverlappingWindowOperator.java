package org.edu.snu.tempest.operator;

import org.apache.reef.wake.EventHandler;
import org.edu.snu.tempest.operator.impl.LogicalTime;

import org.edu.snu.tempest.Timescale;


/**
 * OverlappingWindowOperator.
 *
 * It does final aggregation per interval.
 * For example, if a timescale is [w=10s, i=3s],
 * then OWO produces an output with 10 seconds window size every 3 seconds.
 */
public interface OverlappingWindowOperator<V> extends EventHandler<LogicalTime> {
  Timescale getTimescale();
}
