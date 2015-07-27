package edu.snu.tempest.operators.common;

import java.util.List;

/**
 * Aggregation function for multi-time scale aggregation.
 */
public interface Aggregator<I, V> {

  /**
   * Initialization function when the data is new.
   * @return initial output.
   */
  V init();

  /**
   * Aggregate the new data with aggOutput.
   * @param aggOutput an aggregated output.
   * @param newVal new value
   * @return partially aggregated data.
   */
  V partialAggregate(final V aggOutput, final I newVal);

  /**
   * Final aggregation of the partial results.
   * @param partials a list of outputs of partial aggregation.
   * @return an output aggregating the list of outputs.
   */
  V finalAggregate(List<V> partials);
}