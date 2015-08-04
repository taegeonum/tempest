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

import edu.snu.tempest.operator.window.WindowOperator;
import edu.snu.tempest.operator.window.aggregator.CAAggregator;
import edu.snu.tempest.operator.window.time.impl.MTSOperatorImpl;
import edu.snu.tempest.operator.window.time.parameter.StartTime;
import edu.snu.tempest.operator.window.time.parameter.TimescaleString;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.RequiredImpl;
import org.apache.reef.tang.formats.RequiredParameter;

/**
 * A helper class for TimeWindowOperator configuration.
 */
public class TimeWindowBaseConfiguration extends ConfigurationModuleBuilder {

  /**
   * Start time of the operator.
   */
  public static final RequiredParameter<Long> START_TIME = new RequiredParameter<>();

  /**
   * A commutative/associative aggregator.
   */
  public static final RequiredImpl<CAAggregator> CA_AGGREGATOR = new RequiredImpl<>();

  /**
   * An output handler.
   */
  public static final RequiredImpl<TimeWindowOutputHandler> OUTPUT_HANDLER = new RequiredImpl<>();

  /**
   * Initial timescales.
   */
  public static final RequiredParameter<String> INITIAL_TIMESCALES = new RequiredParameter<>();

  public static final ConfigurationModule CONF = new TimeWindowBaseConfiguration()
      .bindNamedParameter(StartTime.class, START_TIME)
      .bindNamedParameter(TimescaleString.class, INITIAL_TIMESCALES)
      .bindImplementation(TimeWindowOutputHandler.class, OUTPUT_HANDLER)
      .bindImplementation(CAAggregator.class, CA_AGGREGATOR)
      .bindImplementation(WindowOperator.class, MTSOperatorImpl.class)
      .build();
}
