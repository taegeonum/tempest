package org.edu.snu.tempest.operators.dynamicmts.signal;


import org.edu.snu.tempest.operators.Timescale;

public final class TimescaleSignal {

  public final Timescale ts;
  public final long startTime;

  public TimescaleSignal(final Timescale ts, final long startTime) {
    this.ts = ts;
    this.startTime = startTime;
  }
}
