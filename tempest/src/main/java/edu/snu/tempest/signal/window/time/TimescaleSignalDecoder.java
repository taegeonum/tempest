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
package edu.snu.tempest.signal.window.time;

import com.google.protobuf.InvalidProtocolBufferException;
import edu.snu.tempest.proto.TimescaleProtoMessage;
import org.apache.reef.wake.remote.Decoder;

import javax.inject.Inject;

/**
 * Decoder for TimescaleSignal.
 */
public final class TimescaleSignalDecoder implements Decoder<TimescaleSignal> {

  @Inject
  private TimescaleSignalDecoder() {
  }
  
  @Override
  public TimescaleSignal decode(final byte[] data) {
    try {
      final TimescaleProtoMessage.TimescaleSignal signal =
          TimescaleProtoMessage.TimescaleSignal.parseFrom(data);
      return new TimescaleSignal(signal.getWindowSize(),
          signal.getInterval(), signal.getType(), signal.getStartTime());
    } catch (final InvalidProtocolBufferException e) {
      e.printStackTrace();
      throw new RuntimeException("TimescaleDecoder decode error");
    }
  }

}
