package edu.snu.tempest.operator.window.aggregator;


import java.util.Collection;

/**
 * Final aggregator for commutative/associative aggregation.
 * This can be used when the aggregator is commutative/associative.
 * @param <V> result
 */
public interface FinalAggregator<V> {
  /**
   * Final aggregation of the aggregated results.
   * @param partials a list of buckets of partial aggregation.
   * @return an output aggregating the list of buckets.
   */
  V finalAggregate(Collection<V> partials);
}