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


import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Test util class.
 */
public final class MTSTestUtils {

  /**
   * Merge map.
   * @param map1 initial map
   * @param maps other maps
   * @return merged map
   */
  public static Map<Integer, Long> merge(final Map<Integer, Long> map1, final Map<Integer, Long> ... maps) {
    final Map<Integer, Long> result = new HashMap<>();
    result.putAll(map1);

    for (final Map<Integer, Long> map2 : maps) {
      for (final Map.Entry<Integer, Long> entry : map2.entrySet()) {
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
   * Merge map.
   * @param maps maps
   * @return merged map
   */
  public static Map<Integer, Long> merge(final List<Map<Integer, Long>> maps) {
    final Map<Integer, Long> result = new HashMap<>();
    for (final Map<Integer, Long> map : maps) {
      for (final Map.Entry<Integer, Long> entry : map.entrySet()) {
        Long oldVal = result.get(entry.getKey());
        if (oldVal == null) {
          oldVal = 0L;
        }
        result.put(entry.getKey(), oldVal + entry.getValue());
      }
    }
    return result;
  }
}
