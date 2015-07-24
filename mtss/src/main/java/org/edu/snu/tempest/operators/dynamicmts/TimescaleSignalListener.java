package org.edu.snu.tempest.operators.dynamicmts;

import org.edu.snu.tempest.operators.Timescale;

/**
 * Dynamically add/remove timescales.
 */
public interface TimescaleSignalListener {
  // time unit is second.
  void onTimescaleAddition(Timescale ts, final long startTime);
  void onTimescaleDeletion(Timescale ts);
}
