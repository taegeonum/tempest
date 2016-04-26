package vldb.operator.window.timescale.pafas;

import vldb.operator.window.timescale.Timescale;
import vldb.operator.window.timescale.common.TimescaleParser;

import javax.inject.Inject;
import java.util.List;

public final class PeriodCalculator {

  private final long period;

  @Inject
  private PeriodCalculator(final TimescaleParser tsParser) {
    this.period = calculatePeriod(tsParser.timescales);
    System.out.println("@Period: " + period);
  }

  public long getPeriod() {
    return period;
  }

  /**
   * Find period of repeated pattern.
   * period = c * lcm ( i_{1}, i_{2}, ..., i_{k} ) ( i_{k} is interval of k-th timescale)
   * c is natural number which satisfies period >= largest_window_size
   */
  private long calculatePeriod(final List<Timescale> timescales) {
    long period = 0;
    long largestWindowSize = 0;

    for (final Timescale ts : timescales) {
      if (period == 0) {
        period = ts.intervalSize;
      } else {
        period = lcm(period, ts.intervalSize);
      }
      // find largest window size
      if (largestWindowSize < ts.windowSize) {
        largestWindowSize = ts.windowSize;
      }
    }

    if (period < largestWindowSize) {
      final long div = largestWindowSize / period;
      if (largestWindowSize % period == 0) {
        period *= div;
      } else {
        period *= (div+1);
      }
    }
    return period;
  }

  private static long gcd(long a, long b) {
    while (b > 0) {
      final long temp = b;
      b = a % b; // % is remainder
      a = temp;
    }
    return a;
  }

  private static long lcm(final long a, final long b) {
    return a * (b / gcd(a, b));
  }

}
