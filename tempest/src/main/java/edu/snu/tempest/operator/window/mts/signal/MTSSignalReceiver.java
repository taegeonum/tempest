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
package edu.snu.tempest.operator.window.mts.signal;

    import edu.snu.tempest.operator.window.mts.TimescaleSignalListener;
    import edu.snu.tempest.operator.window.mts.signal.impl.ZkSignalReceiver;
    import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * Receiver for runtime multiple timescale addition/deletion.
 * It receives timescale addition/deletion messages from MTSSignalSender
 * and triggers TimescaleSignalListener.onTimescaleAddition / onTimescaleDeletion
 */
@DefaultImplementation(ZkSignalReceiver.class)
public interface MTSSignalReceiver extends AutoCloseable {

  void start() throws Exception;

  /**
   * MTSSignalReceiver sends timescale information to TimescaleSignalListener.
   * @param listener timescale signal listener
   */
  void addTimescaleSignalListener(TimescaleSignalListener listener);
}
