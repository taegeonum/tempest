package org.edu.snu.tempest.operators.common.aggregators;

import org.edu.snu.tempest.operators.common.Aggregator;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CountByKeyAggregator<I, K> implements Aggregator<I, Map<K, Long>> {

  private final KeyFromValue<I, K> keyVal;

  @Inject
  public CountByKeyAggregator(KeyFromValue<I, K> keyVal) {
    this.keyVal = keyVal;
  }

  @Override
  public Map<K, Long> init() {
    return new HashMap<>();
  }

  @Override
  public Map<K, Long> partialAggregate(Map<K, Long> oldVal, I newVal) {
    K key = keyVal.getKey(newVal);
    Long old = oldVal.get(key);

    if (old == null) {
      old = 0L;
    }
    oldVal.put(key, old + 1);
    return oldVal;
  }

  @Override
  public Map<K, Long> finalAggregate(List<Map<K, Long>> partials) {
    Map<K, Long> result = new HashMap<>();
    for (Map<K, Long> partial : partials) {
      for (Map.Entry<K, Long> entry : partial.entrySet()) {
        Long oldVal = result.get(entry.getKey());
        if (oldVal == null) {
          oldVal = 0L;
        }
        result.put(entry.getKey(), oldVal + entry.getValue());
      }
    }
    return result;
  }

  public interface KeyFromValue<I, K> {
    K getKey(I value);
  }
}
