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