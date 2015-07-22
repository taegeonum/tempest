package org.edu.snu.tempest.operators.dynamicmts.signal;

import org.edu.snu.tempest.operators.Timescale;

public interface TimescaleSignalListener {

  // time unit is second.
  void onTimescaleAddition(Timescale ts, final long startTime);
  void onTimescaleDeletion(Timescale ts);
}
