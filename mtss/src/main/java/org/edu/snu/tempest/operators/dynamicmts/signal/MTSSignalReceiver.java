package org.edu.snu.tempest.operators.dynamicmts.signal;

import org.apache.reef.tang.annotations.DefaultImplementation;
import org.edu.snu.tempest.operators.dynamicmts.TimescaleSignalListener;
import org.edu.snu.tempest.operators.dynamicmts.signal.impl.ZkSignalReceiver;

/**
 * Receiver for runtime multiple timescale addition/deletion.
 * It receives timescale addition/deletion messages from MTSSignalSender 
 * and triggers TimescaleSignalListener.onTimescaleAddition / onTimescaleDeletion 
 */
@DefaultImplementation(ZkSignalReceiver.class)
public interface MTSSignalReceiver extends AutoCloseable {

  void start() throws Exception;

  /**
   * MTSSignalReceiver sends timescale information to TimescaleSignalListener.
   * @param listener timescale signal listener
   */
  void addTimescaleSignalListener(TimescaleSignalListener listener);
}
