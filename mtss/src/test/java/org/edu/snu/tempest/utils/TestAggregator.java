package org.edu.snu.tempest.utils;

import org.edu.snu.tempest.operator.MTSOperator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


public final class TestAggregator implements MTSOperator.Aggregator<Integer, Map<Integer, Integer>> {

  @Override
  public Map<Integer, Integer> init() {
    return new HashMap<>();
  }

  @Override
  public Map<Integer, Integer> partialAggregate(Map<Integer, Integer> oldVal, Integer newVal) {
    Integer val = oldVal.get(newVal);
    if (val == null) {
      val = 0;
    }

    oldVal.put(newVal, val + 1);
    return oldVal;
  }

  @Override
  public Map<Integer, Integer> finalAggregate(List<Map<Integer, Integer>> partials) {
    Map<Integer, Integer> result = new HashMap<>();

    for (Map<Integer, Integer> partial : partials) {
      if (result.size() == 0) {
        result.putAll(partial);
      } else {
        for (Map.Entry<Integer, Integer> entry : partial.entrySet()) {
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