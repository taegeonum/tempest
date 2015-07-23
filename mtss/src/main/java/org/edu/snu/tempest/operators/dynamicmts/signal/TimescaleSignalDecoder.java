package org.edu.snu.tempest.operators.dynamicmts.signal;

import com.google.protobuf.InvalidProtocolBufferException;
import edu.snu.tempest.proto.TimescaleProtoMessage;
import org.apache.reef.wake.remote.Decoder;
import org.edu.snu.tempest.operators.Timescale;

import javax.inject.Inject;

public class TimescaleSignalDecoder implements Decoder<TimescaleSignal> {

  @Inject
  public TimescaleSignalDecoder() {
    
  }
  
  @Override
  public TimescaleSignal decode(byte[] data) {
    try {
      TimescaleProtoMessage.TimescaleSignal signal =
          TimescaleProtoMessage.TimescaleSignal.parseFrom(data);
      return new TimescaleSignal(
          new Timescale((int)signal.getWindowSize(), (int)signal.getInterval()),
              signal.getStartTime());
    } catch (InvalidProtocolBufferException e) {
      e.printStackTrace();
      throw new RuntimeException("TimescaleDecoder decode error");
    }
  }

}
