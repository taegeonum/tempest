/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package edu.snu.tempest.operator.window.aggregator.impl;

import edu.snu.tempest.operator.window.aggregator.ComAndAscAggregator;

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
public final class ComputeByKeyAggregator<I, K, V> implements ComAndAscAggregator<I, Map<K, V>> {
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

  /**
   * Compute the input by key.
   * @param keyExtractor a key extractor
   * @param valueExtractor a value extractor
   * @param computeFunc a compute function
   */
  @Inject
  private ComputeByKeyAggregator(final KeyExtractor<I, K> keyExtractor,
                                final ValueExtractor<I, V> valueExtractor,
                                final ComputeByKeyFunc<V> computeFunc) {
    this.keyExtractor = keyExtractor;
    this.valueExtractor = valueExtractor;
    this.computeFunc = computeFunc;
  }

  /**
   * Create a new bucket for partial aggregation.
   * @return a map
   */
  @Override
  public Map<K, V> init() {
    return new HashMap<>();
  }

  /**
   * Counts the newVal.
   * @param bucket a bucket for partial aggregation.
   * @param newVal new value
   * @return the bucket in which the newVal is counted.
   */
  @Override
  public Map<K, V> partialAggregate(final Map<K, V> bucket, final I newVal) {
    final K key = keyExtractor.getKey(newVal);
    V old = bucket.get(key);

    if (old == null) {
      old = computeFunc.init();
    }
    bucket.put(key, computeFunc.compute(old, valueExtractor.getValue(newVal)));
    return bucket;
  }

  /**
   * Merge the list of buckets to create count by key.
   * @param partials a list of buckets of partial aggregation.
   * @return an output of final aggregation
   */
  @Override
  public Map<K, V> finalAggregate(final Collection<Map<K, V>> partials) {
    final Map<K, V> result = new HashMap<>();
    for (final Map<K, V> partial : partials) {
      for (final Map.Entry<K, V> entry : partial.entrySet()) {
        V oldVal = result.get(entry.getKey());
        if (oldVal == null) {
          oldVal = computeFunc.init();
        }
        result.put(entry.getKey(), computeFunc.compute(oldVal, entry.getValue()));
      }
    }
    return result;
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
