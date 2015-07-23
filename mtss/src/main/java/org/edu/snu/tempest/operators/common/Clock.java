package org.edu.snu.tempest.operators.common;

import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.wake.Stage;
import org.edu.snu.tempest.operators.Timescale;
import org.edu.snu.tempest.operators.common.impl.DefaultMTSClockImpl;

/**
 * Clock for MTS operator.
 * It schedules the execution of SlicedWindowOperator and OverlappingWindowOperator.
 * 
 */
@DefaultImplementation(DefaultMTSClockImpl.class)
public interface Clock extends Stage {

  /**
   * Start clock.
   */
  void start();
  
  /**
   * Subscribe OverlappingWindowOperator and returns Subscription.
   * 
   * @param overlappingWindowoperator a new OverlappingWindowOperator
   */
  Subscription<Timescale> subscribe(final OverlappingWindowOperator<?> overlappingWindowoperator);
  
  /**
   * Get current time.
   */
  long getCurrentTime();
}
