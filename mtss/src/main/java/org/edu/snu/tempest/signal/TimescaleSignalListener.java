package org.edu.snu.tempest.signal;

import org.edu.snu.tempest.Timescale;

public interface TimescaleSignalListener {

  // time unit is second.
  void onTimescaleAddition(Timescale ts, final long startTime);
  void onTimescaleDeletion(Timescale ts);
}
