package org.edu.snu.tempest.operators.common;

import java.util.List;

/**
 * Aggregation function for multi-time scale aggregation.
 */
public interface Aggregator<I, V> {

  /**
   * Initialization function when the data is new.
   *
   */
  V init();

  /**
   * Partial aggregate the new data.
   *
   * @param oldVal old value
   * @param newVal new value
   */
  V partialAggregate(final V oldVal, final I newVal);

  /**
   * Final aggregation of the partial results.
   * @param partials
   * @return
   */
  V finalAggregate(List<V> partials);
}