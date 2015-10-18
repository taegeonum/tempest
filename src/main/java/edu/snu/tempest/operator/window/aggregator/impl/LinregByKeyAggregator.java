package edu.snu.tempest.operator.window.aggregator.impl;

import edu.snu.tempest.operator.window.aggregator.CAAggregator;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;

import javax.inject.Inject;
import java.util.Collection;
import java.util.Map;

/**
 * CountByKeyAggregator.
 * It counts input by key.
 * @param <I> input
 * @param <K> key
 */
public final class LinregByKeyAggregator<I, K> implements CAAggregator<I, Map<K, LinregVal>> {
  /**
   * ComputeByKeyAggregator for countByKey.
   */
  private final ComputeByKeyAggregator<I, K, LinregVal> aggregator;

  /**
   * Count the input by key.
   * @param extractor a key extractor
   */
  @Inject
  private LinregByKeyAggregator(final KeyExtractor<I, K> extractor,
                                final ValueExtractor<I, Long> valueExtractor) throws InjectionException {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindImplementation(ComputeByKeyAggregator.ComputeByKeyFunc.class, LinregFunc.class);
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
  public Map<K, LinregVal> init() {
    return this.aggregator.init();
  }

  /**
   * Counts the newVal.
   * @param bucket a bucket for incremental aggregation.
   * @param newVal new value
   */
  @Override
  public void incrementalAggregate(final Map<K, LinregVal> bucket, final I newVal) {
    this.aggregator.incrementalAggregate(bucket, newVal);
  }

  /**
   * Merge the list of buckets to create count by key.
   * @param partials a list of buckets of incremental aggregation.
   * @return an output of final aggregation
   */
  @Override
  public Map<K, LinregVal> aggregate(final Collection<Map<K, LinregVal>> partials) {
    return this.aggregator.aggregate(partials);
  }
}
