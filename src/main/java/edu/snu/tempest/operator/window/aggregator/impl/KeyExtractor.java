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

import org.apache.reef.tang.annotations.DefaultImplementation;
import vldb.example.DefaultExtractor;

/**
 * Extract key from input.
 * @param <I> input
 * @param <K> key
 */
@DefaultImplementation(DefaultExtractor.class)
public interface KeyExtractor<I, K> {
  /**
   * Get key from the input.
   * @param input input value
   * @return key
   */
  K getKey(I input);
}