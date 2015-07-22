package org.edu.snu.tempest.operator;

import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.wake.Stage;
import org.edu.snu.tempest.Timescale;
import org.edu.snu.tempest.operator.impl.DefaultMTSClockImpl;

/**
 * Clock for window operators 
 * It first notifies logical time to SlicedWindowOperator.
 * After that, it notifies the logical time to all the OverlappingWindowOperators. 
 * 
 */
@DefaultImplementation(DefaultMTSClockImpl.class)
public interface Clock extends Stage {

  /*
   * Start clock
   */
  void start();
  
  /*
   * Subscribe OverlappingWindowOperator and returns Subscription
   * 
   * @param overlappingWindowoperator a new OverlappingWindowOperator
   */
  Subscription<Timescale> subscribe(final OverlappingWindowOperator<?> overlappingWindowoperator);
  
  /*
   * Get current logical time
   */
  long getCurrentTime();
}
