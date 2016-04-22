package vldb.operator.window.aggregator.impl;

import javax.inject.Inject;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by taegeonum on 9/25/15.
 */
public final class UniqComputeFunc<V> implements ComputeByKeyAggregator.ComputeByKeyFunc<Set<V>> {

  @Inject
  private UniqComputeFunc() {

  }

  @Override
  public Set<V> init() {
    return new HashSet<>();
  }

  @Override
  public Set<V> compute(final Set<V> oldVal, final Set<V> newVal) {
    final Set<V> set = new HashSet<>();
    set.addAll(oldVal);
    set.addAll(newVal);
    return set;
  }
}
