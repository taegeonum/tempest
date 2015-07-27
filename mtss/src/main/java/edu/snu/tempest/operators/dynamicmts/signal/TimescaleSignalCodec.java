package edu.snu.tempest.operators.dynamicmts.signal;

import org.apache.reef.wake.remote.Codec;

import javax.inject.Inject;

public final class TimescaleSignalCodec implements Codec<TimescaleSignal> {

  /**
   * An encoder of TimescaleSignal.
   */
  private final TimescaleSignalEncoder encoder;

  /**
   * A decoder of TimescaleSignal.
   */
  private final TimescaleSignalDecoder decoder;

  /**
   * TimescaleSignalCodec.
   */
  @Inject
  public TimescaleSignalCodec() {
    this.encoder = new TimescaleSignalEncoder();
    this.decoder = new TimescaleSignalDecoder();
  }
  
  @Override
  public byte[] encode(final TimescaleSignal ts) {
    return encoder.encode(ts);
  }

  @Override
  public TimescaleSignal decode(final byte[] data) {
    return decoder.decode(data);
  }

}
