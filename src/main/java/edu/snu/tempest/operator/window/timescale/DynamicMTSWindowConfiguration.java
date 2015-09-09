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

import edu.snu.tempest.operator.window.timescale.impl.*;
import edu.snu.tempest.operator.window.timescale.parameter.CachingRate;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.OptionalParameter;

/**
 * A helper class for dynamic MTS window configuration.
 */
public final class DynamicMTSWindowConfiguration extends TimescaleWindowBaseConfiguration {

  /**
   * A caching rate for dynamic mts operator.
   */
  public static final OptionalParameter<Double> CACHING_RATE = new OptionalParameter<>();

  public static final ConfigurationModule CONF = new DynamicMTSWindowConfiguration()
      .merge(TimescaleWindowBaseConfiguration.CONF)
      .bindImplementation(ComputationReuser.class, DynamicComputationReuser.class)
      .bindImplementation(NextSliceTimeProvider.class, DynamicNextSliceTimeProvider.class)
      .bindNamedParameter(CachingRate.class, CACHING_RATE)
      .bindImplementation(CachingPolicy.class, CachingRatePolicy.class)
      .bindImplementation(TimescaleWindowOperator.class, DynamicMTSOperatorImpl.class)
      .bindImplementation(DynamicMTSWindowOperator.class, DynamicMTSOperatorImpl.class)
      .build();
}
