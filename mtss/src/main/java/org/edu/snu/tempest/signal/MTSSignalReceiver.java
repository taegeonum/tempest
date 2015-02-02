package org.edu.snu.tempest.signal;


/*
 * Receiver for runtime multiple timescale addition/deletion 
 * It receives timescale addition/deletion messages from MTSSignalSender 
 * and triggers TimescaleSignalListener.onTimescaleAddition / onTimescaleDeletion 
 */
public interface MTSSignalReceiver extends AutoCloseable {

  public void start() throws Exception;
}
