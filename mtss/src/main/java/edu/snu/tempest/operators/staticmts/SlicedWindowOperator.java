package edu.snu.tempest.operators.staticmts;

import org.apache.reef.wake.EventHandler;

/**
 * Sliced window operator.
 * It chops input stream and aggregates the input using Aggregator.
 * After that, it saves the partially aggregated results into RelationCube.
 */
public interface SlicedWindowOperator<I> extends EventHandler<Long> {

  /**
   * Aggregate input.
   */
  void execute(final I val);
}
