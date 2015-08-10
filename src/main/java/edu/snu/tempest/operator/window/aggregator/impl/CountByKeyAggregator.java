/*
 * Copyright (C) 2015 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
public final class CountByKeyAggregator<I, K> implements CAAggregator<I, Map<K, Long>> {
  /**
   * ComputeByKeyAggregator for countByKey.
   */
  private final ComputeByKeyAggregator<I, K, Long> aggregator;

  /**
   * Count the input by key.
   * @param extractor a key extractor
   */
  @Inject
  private CountByKeyAggregator(final KeyExtractor<I, K> extractor) throws InjectionException {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindImplementation(ValueExtractor.class, CountByKeyValueExtractor.class);
    jcb.bindImplementation(ComputeByKeyAggregator.ComputeByKeyFunc.class, CountByKeyComputeFunc.class);
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    injector.bindVolatileInstance(KeyExtractor.class, extractor);
    this.aggregator = injector.getInstance(ComputeByKeyAggregator.class);
  }

  /**
   * Create a new bucket for incremental aggregation.
   * @return a map
   */
  @Override
  public Map<K, Long> init() {
    return this.aggregator.init();
  }

  /**
   * Counts the newVal.
   * @param bucket a bucket for incremental aggregation.
   * @param newVal new value
   */
  @Override
  public void incrementalAggregate(final Map<K, Long> bucket, final I newVal) {
    this.aggregator.incrementalAggregate(bucket, newVal);
  }

  /**
   * Merge the list of buckets to create count by key.
   * @param partials a list of buckets of incremental aggregation.
   * @return an output of final aggregation
   */
  @Override
  public Map<K, Long> aggregate(final Collection<Map<K, Long>> partials) {
    return this.aggregator.aggregate(partials);
  }
}
