package edu.snu.tempest.test.util;

import org.apache.reef.wake.remote.Codec;

import javax.inject.Inject;

/**
 * A codec for test signal.
 */
public final class TestSignalCodec implements Codec<TestSignal> {

  @Inject
  private TestSignalCodec() {
  }

  @Override
  public TestSignal decode(final byte[] bytes) {
    return new TestSignal();
  }

  @Override
  public byte[] encode(final TestSignal testSignal) {
    return new byte[0];
  }
}
