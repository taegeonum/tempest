package atc.evaluation.util;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;
import atc.operator.window.timescale.Timescale;
import atc.operator.window.timescale.pafas.PeriodCalculator;

import javax.inject.Inject;
import java.util.*;

/**
 * Created by taegeonum on 4/25/16.
 */
public final class RandomSlidingWindowGenerator implements SlidingWindowGenerator {

  @NamedParameter
  public static final class MinWindowSize implements Name<Integer> {}

  @NamedParameter
  public static final class MaxWindowSize implements Name<Integer> {}

  @NamedParameter
  public static final class MinIntervalSize implements Name<Integer> {}

  @NamedParameter
  public static final class MaxIntervalSize implements Name<Integer> {}

  @NamedParameter(default_value = "9223372036854775807")
  public static final class MaxPeriod implements Name<Long> {}

  private final int minWindowSize;
  private final int maxWindowSize;
  private final int minIntervalSize;
  private final int maxIntervalSize;
  private final List<Integer> intervals = new LinkedList<>();
  private final long maxPeriod;

  @Inject
  private RandomSlidingWindowGenerator(
      @Parameter(MinWindowSize.class) final int minWindowSize,
      @Parameter(MaxWindowSize.class) final int maxWindowSize,
      @Parameter(MinIntervalSize.class) final int minIntervalSize,
      @Parameter(MaxIntervalSize.class) final int maxIntervalSize,
      @Parameter(MaxPeriod.class) final long maxPeriod) {
    assert minWindowSize > 0 && minWindowSize < maxWindowSize;
    assert minIntervalSize > 0 && minIntervalSize < maxIntervalSize;
    this.minWindowSize = minWindowSize;
    this.maxIntervalSize= maxIntervalSize;
    this.minIntervalSize = minIntervalSize;
    this.maxWindowSize = maxWindowSize;
    this.maxPeriod = maxPeriod;
  }

  public List<Integer> getIntervals(final int num) {
    final Random random = new Random();
    final List<Integer> intervals = new LinkedList<>();

    for (int i = 0; i < num; i++) {
      while (true) {
        final int intervalRange = maxIntervalSize - minIntervalSize;
        final int intervalSize = (Math.abs(random.nextInt()) % (intervalRange + 1)) + minIntervalSize;
        intervals.add(intervalSize);
        if (PeriodCalculator.calculatePeriodFromIntervals(intervals) > maxPeriod) {
          intervals.remove((Integer)intervalSize);
        } else {
          break;
        }
      }
    }
    return intervals;
  }

  public List<Integer> getWindows(final int num) {
    final Random random = new Random();
    final Set<Integer> windows = new HashSet<>();

    while (windows.size() < num) {
      final int windowRange = maxWindowSize - minWindowSize;
      final int windowSize = (Math.abs(random.nextInt()) % (windowRange+1)) + minWindowSize;
      windows.add(windowSize);
    }
    return new LinkedList<>(windows);
  }


  public List<Timescale> generateSlidingWindows(final int num) {
    final Set<Integer> windowSizes = new HashSet<>();
    final Random random = new Random();
    final Set<Timescale> timescales = new HashSet<>();
    final int windowRange = maxWindowSize - minWindowSize;
    final int intervalRange = maxIntervalSize - minIntervalSize;
    while (timescales.size() < num) {
      //final int windowSize = (Math.abs(random.nextInt()) % (windowRange+1)) + minWindowSize;
      //final int intervalSize = (Math.abs(random.nextInt()) % (intervalRange+1)) + minIntervalSize;
      int windowSize = (Math.abs(random.nextInt()) % (windowRange+1)) + minWindowSize;
      int intervalSize = (Math.abs(random.nextInt()) % (intervalRange+1)) + minIntervalSize;
      //windowSize = windowSize - windowSize % 10;
      //intervalSize = Math.max(1, intervalSize - intervalSize % 5);
      if (windowSize > intervalSize) {
        if (!windowSizes.contains(windowSize)) {
          windowSizes.add(windowSize);
          final Timescale ts = new Timescale(windowSize, intervalSize);
          timescales.add(ts);
          if (PeriodCalculator.calculatePeriodFromTimescales(timescales) > (Long.MAX_VALUE/300)) {
          //if (PeriodCalculator.calculatePeriodFromTimescales(timescales) > maxPeriod) {
            timescales.remove(ts);
          }
        }
      }
    }
    //System.out.println("Perid: " + PeriodCalculator.calculatePeriodFromTimescales(timescales));
    return new LinkedList<>(timescales);
  }

  public List<Timescale> generateSlidingWindowsWithFixedWindows(final List<Integer> windows) {
    final Random random = new Random();
    final Set<Timescale> timescales = new HashSet<>();
    final int intervalRange = maxIntervalSize - minIntervalSize;
    for (final long windowSize : windows) {
      while (true) {
        final int intervalSize = (Math.abs(random.nextInt()) % (intervalRange+1)) + minIntervalSize;
        if (windowSize > intervalSize) {
          final Timescale ts = new Timescale(windowSize, intervalSize);
          timescales.add(ts);
          if (PeriodCalculator.calculatePeriodFromTimescales(timescales) > maxPeriod) {
            timescales.remove(ts);
          } else {
            break;
          }
        }
      }
    }
    return new LinkedList<>(timescales);
  }

  public List<Timescale> generateSlidingWindowsWithFixedInterval(final List<Integer> intervals) {
    final Random random = new Random();
    final Set<Integer> windowSizes = new HashSet<>();
    final Set<Timescale> timescales = new HashSet<>();
    final int windowRange = maxWindowSize - minWindowSize;
    for (final long interval : intervals) {
      while (true) {
        final int windowSize = (Math.abs(random.nextInt()) % (windowRange+1)) + minWindowSize;
        if (windowSize > interval) {
          if (!windowSizes.contains(windowSize)) {
            windowSizes.add(windowSize);
            timescales.add(new Timescale(windowSize, interval));
            break;
          }
        }
      }
    }
    return new LinkedList<>(timescales);
  }
}
