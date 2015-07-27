package edu.snu.tempest.operators.dynamicmts.signal;

import edu.snu.tempest.operators.Timescale;

/**
 * TimescaleSignal containing timescale and start time information.
 */
public final class TimescaleSignal {
  public final Timescale ts;
  public final long startTime;

  /**
   * TimescaleSignal for dynamic addition/deletion.
   * @param ts a timescale
   * @param startTime start time of the addition/deletion
   */
  public TimescaleSignal(final Timescale ts, final long startTime) {
    this.ts = ts;
    this.startTime = startTime;
  }
}
