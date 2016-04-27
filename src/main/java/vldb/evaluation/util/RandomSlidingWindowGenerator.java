package vldb.evaluation.util;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;
import vldb.operator.window.timescale.Timescale;

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

  private final int minWindowSize;
  private final int maxWindowSize;
  private final int minIntervalSize;
  private final int maxIntervalSize;
  private final Set<Integer> windowSizes = new HashSet<>();

  @Inject
  private RandomSlidingWindowGenerator(
      @Parameter(MinWindowSize.class) final int minWindowSize,
      @Parameter(MaxWindowSize.class) final int maxWindowSize,
      @Parameter(MinIntervalSize.class) final int minIntervalSize,
      @Parameter(MaxIntervalSize.class) final int maxIntervalSize) {
    assert minWindowSize > 0 && minWindowSize < maxWindowSize;
    assert minIntervalSize > 0 && minIntervalSize < maxIntervalSize;
    this.minWindowSize = minWindowSize;
    this.maxIntervalSize= maxIntervalSize;
    this.minIntervalSize = minIntervalSize;
    this.maxWindowSize = maxWindowSize;
  }

  public List<Timescale> generateSlidingWindows(final int num) {
    final Random random = new Random();
    final Set<Timescale> timescales = new HashSet<>();
    final int windowRange = maxWindowSize - minWindowSize;
    final int intervalRange = maxIntervalSize - minIntervalSize;

    while (timescales.size() < num) {
      final int w = (Math.abs(random.nextInt()) % (windowRange+5)) + minWindowSize;
      final int windowSize = w - (w%5);
      final int i = (Math.abs(random.nextInt()) % (intervalRange+5)) + minIntervalSize;
      final int intervalSize = i - (i%5);
      if (windowSize > intervalSize) {
        if (!windowSizes.contains(windowSize)) {
          windowSizes.add(windowSize);
          timescales.add(new Timescale(windowSize, intervalSize));
        }
      }
    }
    return new LinkedList<>(timescales);
  }
}
