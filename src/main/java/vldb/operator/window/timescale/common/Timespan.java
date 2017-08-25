package vldb.operator.window.timescale.common;

import vldb.operator.window.timescale.Timescale;

/**
 * Timespan.
 */
public final class Timespan {
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
    if (timescale != null ? !timescale.equals(timespan.timescale) : timespan.timescale != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = (int) (startTime ^ (startTime >>> 32));
    result = 31 * result + (int) (endTime ^ (endTime >>> 32));
    result = 31 * result + (timescale != null ? timescale.hashCode() : 0);
    return result;
  }

  public final long startTime;
  public final long endTime;
  public final Timescale timescale;

  public Timespan(final long startTime, final long endTime, final Timescale timescale) {
    this.startTime = startTime;
    this.endTime = endTime;
    this.timescale = timescale;
  }

  @Override
  public String toString() {
    return "[" + startTime + ", " + endTime + ")";
  }
}
