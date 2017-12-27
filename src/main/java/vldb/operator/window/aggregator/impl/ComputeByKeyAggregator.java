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
package vldb.operator.window.aggregator.impl;

import vldb.evaluation.Metrics;
import vldb.operator.window.aggregator.CAAggregator;

import javax.inject.Inject;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * ComputeByKeyAggregator.
 * It computes input by key.
 * @param <I> input
 * @param <K> key
 */
public final class ComputeByKeyAggregator<I, K, V> implements CAAggregator<I, Map<K, V>> {
  /**
   * Extractor for key.
   */
  private final KeyExtractor<I, K> keyExtractor;

  /**
   * Extractor for value.
   */
  private final ValueExtractor<I, V> valueExtractor;

  /**
   * Compute function.
   */
  private final ComputeByKeyFunc<V> computeFunc;

  private final Metrics metrics;

  /**
   * Compute the input by key.
   * @param keyExtractor a key extractor
   * @param valueExtractor a value extractor
   * @param computeFunc a compute function
   */
  @Inject
  private ComputeByKeyAggregator(final KeyExtractor<I, K> keyExtractor,
                                final ValueExtractor<I, V> valueExtractor,
                                final ComputeByKeyFunc<V> computeFunc,
                                final Metrics metrics) {
    this.keyExtractor = keyExtractor;
    this.valueExtractor = valueExtractor;
    this.computeFunc = computeFunc;
    this.metrics = metrics;
  }

  /**
   * Create a new bucket for incremental aggregation.
   * @return a map
   */
  @Override
  public Map<K, V> init() {
    return new HashMap<>();
  }

  /**
   * Counts the newVal.
   * @param bucket a bucket for incremental aggregation.
   * @param newVal new value
   */
  @Override
  public void incrementalAggregate(final Map<K, V> bucket, final I newVal) {
    final K key = keyExtractor.getKey(newVal);
    V old = bucket.get(key);

    if (old == null) {
      old = computeFunc.init();
    }
    bucket.put(key, computeFunc.compute(old, valueExtractor.getValue(newVal)));
  }

  /**
   * Merge the list of buckets to create count by key.
   * @param partials a list of buckets of incremental aggregation.
   * @return an output of final aggregation
   */
  @Override
  public Map<K, V> aggregate(final Collection<Map<K, V>> partials) {
    final Map<K, V> result = new HashMap<>();
    //long numAgg = 0;
    //System.out.print("FINAL_PARTIALS ");
    for (final Map<K, V> partial : partials) {
      // optimize
      if (partials.size() == 1) {
        return partial;
      }
      //System.out.print(partial.size() + ", ");
      rollup(result, partial);
    }
    //System.out.println("NUMAGG: " + numAgg);
    //aggregationCounter.incrementFinalAggregation(numAgg);
    return result;
  }

  @Override
  public Map<K, V> rollup(final Map<K, V> first, final Map<K, V> second) {
    for (final Map.Entry<K, V> entry : second.entrySet()) {
      V oldVal = first.get(entry.getKey());
      if (oldVal == null) {
        //oldVal = computeFunc.init();
        first.put(entry.getKey(), entry.getValue());
      } else {
        //numAgg += 1;
        first.put(entry.getKey(), computeFunc.compute(oldVal, entry.getValue()));
        //aggregationCounter.incrementFinalAggregation();
        metrics.incrementFinal();
      }
    }
    return first;
  }

  /**
   * Compute by key function.
   * @param <V> computed value
   */
  public interface ComputeByKeyFunc<V> {
    /**
     * Initial value.
     * @return value
     */
    V init();

    /**
     * Compute value.
     * @param oldVal old value
     * @param newVal new value
     * @return computed value
     */
    V compute(V oldVal, V newVal);
  }
}
