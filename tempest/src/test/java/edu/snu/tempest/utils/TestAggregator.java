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
package edu.snu.tempest.utils;


import edu.snu.tempest.operators.common.Aggregator;
import edu.snu.tempest.operators.common.Aggregator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class TestAggregator implements Aggregator<Integer, Map<Integer, Integer>> {

  @Override
  public Map<Integer, Integer> init() {
    return new HashMap<>();
  }

  @Override
  public Map<Integer, Integer> partialAggregate(final Map<Integer, Integer> oldVal,
                                                final Integer newVal) {
    Integer val = oldVal.get(newVal);
    if (val == null) {
      val = 0;
    }

    oldVal.put(newVal, val + 1);
    return oldVal;
  }

  @Override
  public Map<Integer, Integer> finalAggregate(final List<Map<Integer, Integer>> partials) {
    final Map<Integer, Integer> result = new HashMap<>();
    for (final Map<Integer, Integer> partial : partials) {
      if (result.size() == 0) {
        result.putAll(partial);
      } else {
        for (final Map.Entry<Integer, Integer> entry : partial.entrySet()) {
          Integer oldVal = result.get(entry.getKey());
          if (oldVal == null) {
            oldVal = 0;
          }
          result.put(entry.getKey(), oldVal + entry.getValue());
        }
      }
    }
    return result;
  }
}