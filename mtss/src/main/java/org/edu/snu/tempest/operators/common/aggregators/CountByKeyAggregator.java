package org.edu.snu.tempest.operators.common.aggregators;

import org.edu.snu.tempest.operators.common.Aggregator;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * CountByKeyAggregator.
 * It counts input by key.
 * @param <I> input
 * @param <K> key
 */
public final class CountByKeyAggregator<I, K> implements Aggregator<I, Map<K, Long>> {

  private final KeyExtractor<I, K> extractor;

  @Inject
  public CountByKeyAggregator(final KeyExtractor<I, K> extractor) {
    this.extractor = extractor;
  }

  @Override
  public Map<K, Long> init() {
    return new HashMap<>();
  }

  @Override
  public Map<K, Long> partialAggregate(final Map<K, Long> oldVal, final I newVal) {
    final K key = extractor.getKey(newVal);
    Long old = oldVal.get(key);

    if (old == null) {
      old = 0L;
    }
    oldVal.put(key, old + 1);
    return oldVal;
  }

  @Override
  public Map<K, Long> finalAggregate(final List<Map<K, Long>> partials) {
    final Map<K, Long> result = new HashMap<>();
    for (final Map<K, Long> partial : partials) {
      for (final Map.Entry<K, Long> entry : partial.entrySet()) {
        Long oldVal = result.get(entry.getKey());
        if (oldVal == null) {
          oldVal = 0L;
        }
        result.put(entry.getKey(), oldVal + entry.getValue());
      }
    }
    return result;
  }

  public interface KeyExtractor<I, K> {
    K getKey(I value);
  }
}
