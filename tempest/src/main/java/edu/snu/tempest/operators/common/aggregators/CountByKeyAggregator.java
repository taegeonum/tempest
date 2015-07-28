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
package edu.snu.tempest.operators.common.aggregators;

import edu.snu.tempest.operators.common.Aggregator;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * CountByKeyAggregator.
 * It counts input by key.
 * @param <I> input
 * @param <K> key
 */
public final class CountByKeyAggregator<I, K> implements Aggregator<I, Map<K, Long>> {
  /**
   * Key extractor for the input.
   */
  private final KeyExtractor<I, K> extractor;

  /**
   * Count the input by key.
   * @param extractor a key extractor
   */
  @Inject
  public CountByKeyAggregator(final KeyExtractor<I, K> extractor) {
    this.extractor = extractor;
  }

  /**
   * Create a new bucket for partial aggregation.
   * @return a map
   */
  @Override
  public Map<K, Long> init() {
    return new HashMap<>();
  }

  /**
   * Counts the newVal.
   * @param bucket a bucket for partial aggregation.
   * @param newVal new value
   * @return the bucket in which the newVal is counted.
   */
  @Override
  public Map<K, Long> partialAggregate(final Map<K, Long> bucket, final I newVal) {
    final K key = extractor.getKey(newVal);
    Long old = bucket.get(key);

    if (old == null) {
      old = 0L;
    }
    bucket.put(key, old + 1);
    return bucket;
  }

  /**
   * Merge the list of buckets to create count by key.
   * @param partials a list of buckets of partial aggregation.
   * @return an output of final aggregation
   */
  @Override
  public Map<K, Long> finalAggregate(final List<Map<K, Long>> partials) {
    final Map<K, Long> result = new HashMap<>();
    for (final Map<K, Long> partial : partials) {
      for (final Map.Entry<K, Long> entry : partial.entrySet()) {
        Long oldVal = result.get(entry.getKey());
        if (oldVal == null) {
          oldVal = 0L;
        }
        result.put(entry.getKey(), oldVal + entry.getValue());
      }
    }
    return result;
  }

  /**
   * Extract key from input.
   * @param <I> input
   * @param <K> key
   */
  public interface KeyExtractor<I, K> {
    /**
     * Get key from the input.
     * @param value input value
     * @return key
     */
    K getKey(I value);
  }
}
