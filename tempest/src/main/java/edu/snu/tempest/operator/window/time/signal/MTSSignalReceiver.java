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
package edu.snu.tempest.operator.window.time.signal;

/**
 * Receiver for runtime multiple timescale addition/deletion.
 * It receives timescale addition/deletion messages from MTSSignalSender
 * and triggers TimescaleSignalListener.onTimescaleAddition / onTimescaleDeletion
 */
public interface MTSSignalReceiver extends AutoCloseable {

  /**
   * Start receiving signal.
   * @throws Exception an exception when it cannot receive signal.
   */
  void start() throws Exception;

  /**
   * MTSSignalReceiver sends timescale information to TimescaleSignalListener.
   * @param listener timescale signal listener
   */
  void addTimescaleSignalListener(TimescaleSignalListener listener);
}
