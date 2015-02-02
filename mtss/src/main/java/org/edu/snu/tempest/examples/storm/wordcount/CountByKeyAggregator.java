package org.edu.snu.tempest.examples.storm.wordcount;

import backtype.storm.tuple.Tuple;
import org.edu.snu.tempest.operator.MTSOperator;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CountByKeyAggregator<K> implements MTSOperator.Aggregator<Tuple, Map<K, Long>> {

  private final TupleToKey<K> tupleToKey;

  @Inject
  public CountByKeyAggregator(TupleToKey<K> tupleToKey) {
    this.tupleToKey = tupleToKey;
  }

  @Override
  public Map<K, Long> init() {
    return new HashMap<>();
  }

  @Override
  public Map<K, Long> partialAggregate(Map<K, Long> oldVal, Tuple newVal) {
    K key = tupleToKey.getKey(newVal);
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

  public interface TupleToKey<K> {
    K getKey(Tuple tuple);
  }
}
