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

package edu.snu.tempest.operator.window.time;

import edu.snu.tempest.operator.window.time.impl.*;
import edu.snu.tempest.operator.window.time.parameter.CachingRate;
import edu.snu.tempest.operator.window.time.parameter.MTSOperatorIdentifier;
import edu.snu.tempest.signal.impl.ZkMTSParameters;
import edu.snu.tempest.signal.window.time.TimescaleSignalDecoder;
import edu.snu.tempest.signal.window.time.TimescaleSignalEncoder;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.OptionalParameter;
import org.apache.reef.tang.formats.RequiredParameter;

/**
 * A helper class for dynamic MTS window configuration.
 */
public final class DynamicMTSWindowConfiguration extends TimeWindowBaseConfiguration {

  /**
   * A caching rate for dynamic mts operator.
   */
  public static final OptionalParameter<Double> CACHING_RATE = new OptionalParameter<>();

  /**
   * Operator identifier for dynamic mts operator.
   */
  public static final RequiredParameter<String> OPERATOR_IDENTIFIER = new RequiredParameter<>();

  /**
   * Zookeeper address for sending timescale signal.
   */
  public static final RequiredParameter<String> ZK_SERVER_ADDRESS = new RequiredParameter<>();

  public static final ConfigurationModule CONF = new DynamicMTSWindowConfiguration()
      .merge(TimeWindowBaseConfiguration.CONF)
      .bindImplementation(ComputationReuser.class, DynamicComputationReuser.class)
      .bindNamedParameter(CachingRate.class, CACHING_RATE)
      .bindNamedParameter(MTSOperatorIdentifier.class, OPERATOR_IDENTIFIER)
      .bindNamedParameter(ZkMTSParameters.ZkServerAddress.class, ZK_SERVER_ADDRESS)
      .bindNamedParameter(ZkMTSParameters.ZkSignalDecoder.class, TimescaleSignalDecoder.class)
      .bindNamedParameter(ZkMTSParameters.ZkSignalEncoder.class, TimescaleSignalEncoder.class)
      .bindImplementation(CachingPolicy.class, CachingRatePolicy.class)
      .build();
}
