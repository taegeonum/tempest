package edu.snu.tempest.operators.dynamicmts.signal;

import edu.snu.tempest.operators.Timescale;

/**
 * Signal sender for runtime multi-timescale addition/deletion.
 * It sends timescale addition/deletion messages to MTSSignalReceiver 
 */
public interface MTSSignalSender extends AutoCloseable {

  /**
   * Send timescale information to MTSSignalReceiver.
   * @param ts timescale to be added.
   * @throws Exception
   */
  void addTimescale(Timescale ts) throws Exception;

  /**
   * Send timescale information to MTSSignalReceiver.
   * @param ts timescale to be deleted.
   * @throws Exception
   */
  void removeTimescale(Timescale ts) throws Exception;
}
