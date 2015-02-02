package org.edu.snu.tempest.examples.utils;

import com.google.protobuf.InvalidProtocolBufferException;
import org.edu.snu.tempest.Timescale;
import edu.snu.tempest.proto.TimescaleProtoMessage;
import org.apache.reef.wake.remote.Decoder;

import javax.inject.Inject;

public class TimescaleDecoder implements Decoder<Timescale> {

  @Inject
  public TimescaleDecoder() {
    
  }
  
  @Override
  public Timescale decode(byte[] data) {
    try {
      TimescaleProtoMessage.Timescale ts = TimescaleProtoMessage.Timescale.parseFrom(data);
      return new Timescale((int)ts.getWindowSize(), (int)ts.getInterval());
    } catch (InvalidProtocolBufferException e) {
      e.printStackTrace();
      throw new RuntimeException("TimescaleDecoder decode error");
    }
  }

}
