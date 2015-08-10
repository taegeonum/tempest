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
package edu.snu.tempest.test.util;


import edu.snu.tempest.operator.window.aggregator.impl.KeyExtractor;

import javax.inject.Inject;

/**
 * Integer extractor for test.
 */
public final class IntegerExtractor implements KeyExtractor<Integer, Integer> {

  @Inject
  private IntegerExtractor() {
  }

  /**
   * Extract integer.
   * @param value input
   * @return integer
   */
  @Override
  public Integer getKey(final Integer value) {
    return value;
  }
}