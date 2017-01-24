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
import atc.operator.window.timescale.parameter.CachingProb;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.OptionalParameter;

/**
 * A helper class for dynamic MTS window configuration.
 */
public final class DynamicMTSWindowConfiguration extends TimescaleWindowBaseConfiguration {

  /**
   * A caching probability for random caching policy.
   */
  public static final OptionalParameter<Double> CACHING_PROB = new OptionalParameter<>();

  public static final ConfigurationModule CONF = new DynamicMTSWindowConfiguration()
      .merge(TimescaleWindowBaseConfiguration.CONF)
      .bindImplementation(SpanTracker.class, DynamicSpanTracker.class)
      .bindImplementation(NextEdgeProvider.class, DynamicNextEdgeProvider.class)
      .bindNamedParameter(CachingProb.class, CACHING_PROB)
      .bindImplementation(CachingPolicy.class, RandomCachingPolicy.class)
      .bindImplementation(TimescaleWindowOperator.class, DynamicMTSOperatorImpl.class)
      .bindImplementation(DynamicMTSWindowOperator.class, DynamicMTSOperatorImpl.class)
      .build();
}
