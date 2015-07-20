package org.edu.snu.tempest.signal;

import org.edu.snu.tempest.Timescale;

public interface TimescaleSignalListener {

  void onTimescaleAddition(Timescale ts);
  void onTimescaleDeletion(Timescale ts);
}
