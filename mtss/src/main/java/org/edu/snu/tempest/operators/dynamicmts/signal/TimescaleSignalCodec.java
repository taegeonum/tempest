package org.edu.snu.tempest.operators.dynamicmts.signal;

import org.apache.reef.wake.remote.Codec;

import javax.inject.Inject;

public class TimescaleSignalCodec implements Codec<TimescaleSignal> {

  private final TimescaleSignalEncoder encoder;
  private final TimescaleSignalDecoder decoder;
  
  @Inject
  public TimescaleSignalCodec() {
    this.encoder = new TimescaleSignalEncoder();
    this.decoder = new TimescaleSignalDecoder();
  }
  
  @Override
  public byte[] encode(TimescaleSignal ts) {
    return encoder.encode(ts);
  }

  @Override
  public TimescaleSignal decode(byte[] data) {
    return decoder.decode(data);
  }

}
