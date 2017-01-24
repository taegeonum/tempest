package atc.operator.window.aggregator.impl;

import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import atc.operator.window.aggregator.CAAggregator;

import javax.inject.Inject;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Created by taegeonum on 9/25/15.
 */
public final class UniqByKeyAggregator<I, K, V extends Comparable> implements CAAggregator<I, Map<K, Set<V>>> {
  /**
   * ComputeByKeyAggregator for countByKey.
   */
  private final ComputeByKeyAggregator<I, K, Set<V>> aggregator;

  /**
   * Count the input by key.
   * @param extractor a key extractor
   */
  @Inject
  private UniqByKeyAggregator(final KeyExtractor<I, K> extractor,
                             final ValueExtractor<I, V> valueExtractor) throws InjectionException {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindImplementation(ComputeByKeyAggregator.ComputeByKeyFunc.class, UniqComputeFunc.class);
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    injector.bindVolatileInstance(KeyExtractor.class, extractor);
    injector.bindVolatileInstance(ValueExtractor.class, valueExtractor);
    this.aggregator = injector.getInstance(ComputeByKeyAggregator.class);
  }

  /**
   * Create a new bucket for incremental aggregation.
   * @return a map
   */
  @Override
  public Map<K, Set<V>> init() {
    return this.aggregator.init();
  }

  /**
   * Counts the newVal.
   * @param bucket a bucket for incremental aggregation.
   * @param newVal new value
   */
  @Override
  public void incrementalAggregate(final Map<K, Set<V>> bucket, final I newVal) {
    this.aggregator.incrementalAggregate(bucket, newVal);
  }

  /**
   * Merge the list of buckets to create count by key.
   * @param partials a list of buckets of incremental aggregation.
   * @return an output of final aggregation
   */
  @Override
  public Map<K, Set<V>> aggregate(final Collection<Map<K, Set<V>>> partials) {
    return this.aggregator.aggregate(partials);
  }

  @Override
  public Map<K, Set<V>> rollup(final Map<K, Set<V>> first, final Map<K, Set<V>> second) {
    return this.aggregator.rollup(first, second);
  }
}
