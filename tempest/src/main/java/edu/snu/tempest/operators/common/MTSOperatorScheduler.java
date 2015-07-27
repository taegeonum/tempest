package edu.snu.tempest.operators.common;

import edu.snu.tempest.operators.Timescale;
import edu.snu.tempest.operators.common.impl.DefaultMTSOperatorSchedulerImpl;
import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.wake.Stage;

/**
 * Scheduler for MTS operator.
 * It schedules the execution of SlicedWindowOperator and OverlappingWindowOperators
 * according to the order.
 */
@DefaultImplementation(DefaultMTSOperatorSchedulerImpl.class)
public interface MTSOperatorScheduler extends Stage {
  /**
   * Start the scheduler.
   */
  void start();
  
  /**
   * Subscribe an overlapping window operator and return a subscription for unsubscribe.
   * @param overlappingWindowoperator an overlapping window operator.
   * @return a subscription for the overlapping window operator.
   */
  Subscription<Timescale> subscribe(final OverlappingWindowOperator<?> overlappingWindowoperator);
}
