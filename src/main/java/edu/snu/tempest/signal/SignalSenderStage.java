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

import edu.snu.tempest.signal.impl.ZkSignalSenderStage;
import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.wake.Stage;

/**
 * Signal sender for runtime.
 */
@DefaultImplementation(ZkSignalSenderStage.class)
public interface SignalSenderStage<T> extends Stage {

  /**
   * Send signal to the operators which have the identifier.
   * @param identifier an identifier of operators.
   * @param signal a signal
   * @throws Exception
   */
  void sendSignal(String identifier, T signal) throws Exception;
}
