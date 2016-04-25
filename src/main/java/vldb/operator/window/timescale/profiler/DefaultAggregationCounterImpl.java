package vldb.operator.window.timescale.profiler;

import javax.inject.Inject;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class DefaultAggregationCounterImpl implements AggregationCounter {

  private final ConcurrentMap<Integer, Long> paCounterMap;
  private final ConcurrentMap<Integer, Long> faCounterMap;

  private final long startTime;

  private final int numKey = 10;
  private final Random random = new Random();

  @Inject
  private DefaultAggregationCounterImpl() {
    this.paCounterMap = new ConcurrentHashMap<>();
    this.faCounterMap = new ConcurrentHashMap<>();
    for (int i = 0; i < numKey; i++) {
      this.paCounterMap.put(i, 0L);
      this.faCounterMap.put(i, 0L);
    }
    this.startTime = System.currentTimeMillis();
  }

  /**
   */
  @Override
  public void incrementPartialAggregation() {
    while (true) {
      final int key = Math.abs(random.nextInt()) % numKey;
      final Long val = paCounterMap.get(key);
      //System.out.println("KEY: " + key);
      if (paCounterMap.replace(key, val, val + 1)) {
        break;
      }
    }
  }

  @Override
  public void incrementFinalAggregation() {
    while (true) {
      final int key = Math.abs(random.nextInt()) % numKey;
      final Long val = faCounterMap.get(key);
      if (faCounterMap.replace(key, val, val + 1)) {
        break;
      }
    }
  }

  @Override
  public long getNumPartialAggregation() {
    long sum = 0;
    for (final long val : paCounterMap.values()) {
      sum += val;
    }
    return sum;
  }

  @Override
  public long getNumFinalAggregation() {
    long sum = 0;
    for (final long val : faCounterMap.values()) {
      sum += val;
    }
    return sum;
  }
}
