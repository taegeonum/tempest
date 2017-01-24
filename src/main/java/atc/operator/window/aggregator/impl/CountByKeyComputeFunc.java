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
package atc.operator.window.aggregator.impl;


import javax.inject.Inject;

/**
 * Compute function for count by key.
 */
public final class CountByKeyComputeFunc implements ComputeByKeyAggregator.ComputeByKeyFunc<Long> {

  @Inject
  private CountByKeyComputeFunc() {
  }

  /**
   * Returns zero.
   * @return initial value
   */
  @Override
  public Long init() {
    return 0L;
  }

  /**
   * Sum the counts.
   * @param oldVal old value
   * @param newVal new value
   * @return sum
   */
  @Override
  public Long compute(final Long oldVal, final Long newVal) {
    return oldVal + newVal;
  }
}
