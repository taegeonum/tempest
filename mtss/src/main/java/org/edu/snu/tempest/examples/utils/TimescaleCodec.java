package org.edu.snu.tempest.examples.utils;

import org.edu.snu.tempest.Timescale;
import org.apache.reef.wake.remote.Codec;

import javax.inject.Inject;

public class TimescaleCodec implements Codec<Timescale> {

  private final TimescaleEncoder encoder;
  private final TimescaleDecoder decoder;
  
  @Inject
  public TimescaleCodec() {
    this.encoder = new TimescaleEncoder();
    this.decoder = new TimescaleDecoder();
  }
  
  @Override
  public byte[] encode(Timescale ts) {
    return encoder.encode(ts);
  }

  @Override
  public Timescale decode(byte[] data) {
    return decoder.decode(data);
  }

}
