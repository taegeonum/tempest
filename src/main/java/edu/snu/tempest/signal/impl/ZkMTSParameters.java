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
package edu.snu.tempest.signal.impl;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.wake.remote.Decoder;
import org.apache.reef.wake.remote.Encoder;

public final class ZkMTSParameters {
  
  @NamedParameter(doc = "zookeeper server address", short_name = "zkAddress", default_value="localhost:2181")
  public static final class ZkServerAddress implements Name<String> {}

  @NamedParameter(doc = "signal encoder")
  public static final class ZkSignalEncoder implements Name<Encoder<?>> {}

  @NamedParameter(doc = "signal decoder")
  public static final class ZkSignalDecoder implements Name<Decoder<?>> {}
  
  @NamedParameter(doc = "Zookeeper Client data sending period (ms)", default_value = "500") 
  public static final class ZkDataSendingPeriod implements Name<Long> {}

  @NamedParameter(doc = "Zookeeper retry times", default_value = "3")
  public static final class ZkRetryTimes implements Name<Integer> {}
  
  @NamedParameter(doc = "Zookeeper retry interval", default_value = "500")
  public static final class ZkRetryPeriod implements Name<Integer> {}
}
