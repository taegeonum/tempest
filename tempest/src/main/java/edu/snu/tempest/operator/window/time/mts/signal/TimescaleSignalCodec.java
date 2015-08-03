/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package edu.snu.tempest.operator.window.time.mts.signal;

import org.apache.reef.wake.remote.Codec;

import javax.inject.Inject;

/**
 * Codec for TimescaleSignal.
 */
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
   * Codec for TimescaleSignal.
   * @param encoder encoder of TimescaleSignal
   * @param decoder decoder of TimescaleSignal
   */
  @Inject
  private TimescaleSignalCodec(final TimescaleSignalEncoder encoder,
                               final TimescaleSignalDecoder decoder) {
    this.encoder = encoder;
    this.decoder = decoder;
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
