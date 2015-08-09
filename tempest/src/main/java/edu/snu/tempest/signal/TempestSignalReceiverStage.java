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
package edu.snu.tempest.signal;

import edu.snu.tempest.signal.impl.ZkSignalReceiverStage;
import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Stage;

/**
 * Receiver for runtime signal.
 * It receives signal messages from MTSSignalSender
 * and triggers an event handler which has same identifier
 */
@DefaultImplementation(ZkSignalReceiverStage.class)
public interface TempestSignalReceiverStage<T> extends Stage {

  /**
   * Register a handler for the identifier.
   * @param identifier an identifier
   * @param handler a handler for the signal
   * @throws Exception
   */
  void registerHandler(String identifier, EventHandler<T> handler) throws Exception;
}
