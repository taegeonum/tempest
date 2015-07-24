package org.edu.snu.tempest.operators.dynamicmts.signal;

import edu.snu.tempest.proto.TimescaleProtoMessage;
import org.apache.reef.wake.remote.Encoder;

import javax.inject.Inject;

public class TimescaleSignalEncoder implements Encoder<TimescaleSignal> {

  @Inject
  public TimescaleSignalEncoder() {
    
  }
  
  @Override
  public byte[] encode(final TimescaleSignal signal) {
    final TimescaleProtoMessage.TimescaleSignal encoded = TimescaleProtoMessage.TimescaleSignal.newBuilder()
        .setWindowSize(signal.ts.windowSize)
        .setInterval(signal.ts.intervalSize)
        .setStartTime(signal.startTime)
        .build();
    
    return encoded.toByteArray();
  }
}
