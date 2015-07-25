package org.edu.snu.tempest.operators.dynamicmts;

import org.edu.snu.tempest.operators.Timescale;

/**
 * Dynamically add/remove timescales.
 */
public interface TimescaleSignalListener {
  // time unit is second.
  /**
   * Receive timescale to be added.
   * @param ts timescale to be added.
   * @param startTime the time when timescale is added.
   */
  void onTimescaleAddition(Timescale ts, final long startTime);

  /**
   * Receive timescale to be deleted.
   * @param ts timescale to be deleted.
   */
  void onTimescaleDeletion(Timescale ts);
}
