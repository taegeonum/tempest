package org.edu.snu.tempest.examples.utils;

import org.edu.snu.tempest.Timescale;
import edu.snu.tempest.proto.TimescaleProtoMessage;
import org.apache.reef.wake.remote.Encoder;

import javax.inject.Inject;

public class TimescaleEncoder implements Encoder<Timescale> {

  @Inject
  public TimescaleEncoder() {
    
  }
  
  @Override
  public byte[] encode(Timescale ts) {
    TimescaleProtoMessage.Timescale encoded = TimescaleProtoMessage.Timescale.newBuilder()
        .setWindowSize(ts.windowSize)
        .setInterval(ts.intervalSize)
        .build();
    
    return encoded.toByteArray();
  }

}
