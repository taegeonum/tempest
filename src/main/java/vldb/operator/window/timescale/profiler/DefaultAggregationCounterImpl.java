package vldb.operator.window.timescale.profiler;

import org.apache.reef.tang.annotations.Parameter;
import vldb.evaluation.parameter.EndTime;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class DefaultAggregationCounterImpl implements AggregationCounter {

  private final ConcurrentMap<Integer, Long> paCounterMap;
  private final ConcurrentMap<Integer, Long> faCounterMap;

  private final int numKey = 50;
  private final Random random = new Random();
  private final long endTime;

  @Inject
  private DefaultAggregationCounterImpl(@Parameter(EndTime.class) final long endTime) {
    this.endTime = endTime;
    this.paCounterMap = new ConcurrentHashMap<>();
    this.faCounterMap = new ConcurrentHashMap<>();
    for (int i = 0; i < numKey; i++) {
      this.paCounterMap.put(i, 0L);
      this.faCounterMap.put(i, 0L);
    }
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
  public void incrementFinalAggregation(final long etime, final List<Map> mapList) {
    if (etime <= endTime) {
      long sum = 0;
      for (final Map map : mapList) {
        sum += map.size();
      }

      while (true) {
        final int key = Math.abs(random.nextInt()) % numKey;
        final Long val = faCounterMap.get(key);
        try {
          if (faCounterMap.replace(key, val, val + sum)) {
            break;
          }
        } catch (final NullPointerException e) {
          e.printStackTrace();
          System.out.println("key: " + key + ", val: " + val);
        }
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
