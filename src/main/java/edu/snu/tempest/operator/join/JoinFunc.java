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
package edu.snu.tempest.operator.join;

import java.util.Collection;

/**
 * Join function.
 */
public interface JoinFunc<K, O> {

  /**
   * Join the inputs.
   * @param key join key
   * @param inputs inputs for join
   * @return output of join
   */
  O join(K key, Collection<IdentifierAndValue> inputs);
}