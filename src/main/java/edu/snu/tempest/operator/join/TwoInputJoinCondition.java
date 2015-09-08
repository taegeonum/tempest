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

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;

/**
 * This class is used for join two different inputs.
 * If it receives two inputs having same key, it triggers join.
 */
public final class TwoInputJoinCondition<K> implements JoinCondition<K> {

  /**
   * Counter for counting the number of key input.
   */
  private final Map<K, Integer> counter;

  @Inject
  private TwoInputJoinCondition() {
    this.counter = new HashMap<>();
  }

  /**
   * If current key has two inputs, it is ready to join.
   * @param key join key
   * @param identifierAndValue an identifier and value of join input
   * @return join the inputs having the key or not.
   */
  @Override
  public boolean readyToJoin(final K key, final IdentifierAndValue identifierAndValue) {
    final Integer count = counter.get(key);
    if (count == null) {
      // first input
      counter.put(key, 1);
      return false;
    } else {
      // second input
      return true;
    }
  }

  @Override
  public void reset(final K key) {
    counter.remove(key);
  }
}
