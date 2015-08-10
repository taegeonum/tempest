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

package edu.snu.tempest.operator.window.timescale;

import edu.snu.tempest.operator.window.WindowOperator;
import edu.snu.tempest.operator.window.aggregator.CAAggregator;
import edu.snu.tempest.operator.window.timescale.impl.MTSOperatorImpl;
import edu.snu.tempest.operator.window.timescale.parameter.StartTime;
import edu.snu.tempest.operator.window.timescale.parameter.TimescaleString;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.RequiredImpl;
import org.apache.reef.tang.formats.RequiredParameter;

/**
 * A helper class for TimescaleWindowOperator configuration.
 */
public class TimescaleWindowBaseConfiguration extends ConfigurationModuleBuilder {

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
  public static final RequiredImpl<TimescaleWindowOutputHandler> OUTPUT_HANDLER = new RequiredImpl<>();

  /**
   * Initial timescales.
   */
  public static final RequiredParameter<String> INITIAL_TIMESCALES = new RequiredParameter<>();

  public static final ConfigurationModule CONF = new TimescaleWindowBaseConfiguration()
      .bindNamedParameter(StartTime.class, START_TIME)
      .bindNamedParameter(TimescaleString.class, INITIAL_TIMESCALES)
      .bindImplementation(TimescaleWindowOutputHandler.class, OUTPUT_HANDLER)
      .bindImplementation(CAAggregator.class, CA_AGGREGATOR)
      .bindImplementation(WindowOperator.class, MTSOperatorImpl.class)
      .build();
}
