package atc.operator.window.timescale.common;

import atc.operator.window.timescale.Timescale;

/**
 * Timespan.
 */
public final class Timespan {
  public final long startTime;
  public final long endTime;
  public final Timescale timescale;

  public Timespan(final long startTime, final long endTime, final Timescale timescale) {
    this.startTime = startTime;
    this.endTime = endTime;
    this.timescale = timescale;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final Timespan timespan = (Timespan) o;

    if (endTime != timespan.endTime) {
      return false;
    }
    if (startTime != timespan.startTime) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = (int) (startTime ^ (startTime >>> 32));
    result = 31 * result + (int) (endTime ^ (endTime >>> 32));
    return result;
  }

  @Override
  public String toString() {
    return "[" + startTime + ", " + endTime + ")";
  }
}
