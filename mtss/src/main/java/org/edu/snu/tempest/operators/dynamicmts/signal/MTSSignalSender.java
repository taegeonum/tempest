package org.edu.snu.tempest.operators.dynamicmts.signal;

import org.edu.snu.tempest.operators.Timescale;

/**
 * Signal sender for runtime multi-timescale addition/deletion.
 * It sends timescale addition/deletion messages to MTSSignalReceiver 
 */
public interface MTSSignalSender extends AutoCloseable {
  
  void addTimescale(Timescale ts) throws Exception;
  void removeTimescale(Timescale ts) throws Exception;
}
