package org.edu.snu.tempest.operators.dynamicmts.signal;

import org.edu.snu.tempest.operators.Timescale;

/**
 * TimescaleSignal containing timescale and start time information.
 */
public final class TimescaleSignal {
  public final Timescale ts;
  public final long startTime;

  public TimescaleSignal(final Timescale ts, final long startTime) {
    this.ts = ts;
    this.startTime = startTime;
  }
}
