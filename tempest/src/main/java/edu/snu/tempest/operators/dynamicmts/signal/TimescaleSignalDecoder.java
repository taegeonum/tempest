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
package edu.snu.tempest.operators.dynamicmts.signal;

import com.google.protobuf.InvalidProtocolBufferException;
import edu.snu.tempest.proto.TimescaleProtoMessage;
import org.apache.reef.wake.remote.Decoder;
import edu.snu.tempest.operators.Timescale;

import javax.inject.Inject;

public final class TimescaleSignalDecoder implements Decoder<TimescaleSignal> {

  /**
   * Decode TimescaleSignal.
   */
  @Inject
  public TimescaleSignalDecoder() {
    
  }
  
  @Override
  public TimescaleSignal decode(final byte[] data) {
    try {
      final TimescaleProtoMessage.TimescaleSignal signal =
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
