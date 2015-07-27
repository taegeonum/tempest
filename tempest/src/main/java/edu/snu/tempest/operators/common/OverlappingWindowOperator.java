package edu.snu.tempest.operators.common;

import edu.snu.tempest.operators.Timescale;
import edu.snu.tempest.operators.common.impl.DefaultOverlappingWindowOperatorImpl;
import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.wake.EventHandler;


/**
 * OverlappingWindowOperator.
 *
 * It is triggered and does final aggregation per interval.
 * For example, if a timescale is [w=10s, i=3s],
 * then OWO produces an output with 10 seconds window size every 3 seconds.
 */
@DefaultImplementation(DefaultOverlappingWindowOperatorImpl.class)
public interface OverlappingWindowOperator<V> extends EventHandler<Long> {
  /**
   * Return a timescale related to this overlapping window operator.
   * @return timescale.
   */
  Timescale getTimescale();
}
