package org.edu.snu.tempest.utils;


import java.util.HashMap;
import java.util.Map;

public final class MTSTestUtils {

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
}
