package org.edu.snu.tempest.signal;

import org.edu.snu.tempest.Timescale;

public interface TimescaleSignalListener {

  public void onTimescaleAddition(Timescale ts);
  public void onTimescaleDeletion(Timescale ts);
}
