package vldb.evaluation.common;

import backtype.storm.tuple.Tuple;
import edu.snu.tempest.operator.window.aggregator.CAAggregator;

import javax.inject.Inject;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by taegeonum on 9/16/15.
 */
public final class CompAggregator implements CAAggregator<Tuple, ComputationOutput<Map<String, Long>>> {

  @Inject
  CompAggregator() {

  }

  @Override
  public ComputationOutput<Map<String, Long>> init() {
    final Map<String, Long> map = new HashMap<>();
    return new ComputationOutput<>(0L, 0, map);
  }

  @Override
  public void incrementalAggregate(final ComputationOutput<Map<String, Long>> bucket, final Tuple newVal) {
    final Map<String, Long> b = bucket.output;
    final String word = newVal.getString(0);
    Long old = b.get(word);
    if (old == null) {
      old = 1L;
    }
    b.put(word, old + 1);
  }

  @Override
  public ComputationOutput<Map<String, Long>> aggregate(final Collection<ComputationOutput<Map<String, Long>>> partials) {
    long totalCount = 0;

    final Map<String, Long> result = new HashMap<>();
    for (final ComputationOutput<Map<String, Long>> comp : partials) {
      final Map<String, Long> partial = comp.output;
      for (final Map.Entry<String, Long> entry : partial.entrySet()) {
        Long oldVal = result.get(entry.getKey());
        if (oldVal == null) {
          oldVal = 1L;
        }
        result.put(entry.getKey(), oldVal + 1);
        totalCount++;
      }
    }
    return new ComputationOutput<>(totalCount, partials.size(), result);
  }
}
