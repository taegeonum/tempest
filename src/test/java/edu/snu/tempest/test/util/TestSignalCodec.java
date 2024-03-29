/*
 * Copyright (C) 2015 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
