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
package edu.snu.tempest.operators.common;

import java.util.List;

/**
 * Aggregation function for multi-time scale aggregation.
 */
public interface Aggregator<I, V> {

  /**
   * Create a new bucket for partial aggregation.
   * @return a bucket for aggregation.
   */
  V init();

  /**
   * Aggregate the new data into the bucket.
   * @param bucket a bucket for partial aggregation.
   * @param newVal new value
   * @return a bucket in which the newVal is aggregated
   */
  V partialAggregate(final V bucket, final I newVal);

  /**
   * Final aggregation of the partial results.
   * @param partials a list of buckets of partial aggregation.
   * @return an output aggregating the list of buckets.
   */
  V finalAggregate(List<V> partials);
}