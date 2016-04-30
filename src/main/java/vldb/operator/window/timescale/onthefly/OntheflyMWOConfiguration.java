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
package vldb.operator.window.timescale.onthefly;

import org.apache.reef.tang.formats.ConfigurationModule;
import vldb.operator.window.timescale.TimescaleWindowBaseConfiguration;
import vldb.operator.window.timescale.TimescaleWindowOperator;
import vldb.operator.window.timescale.common.SpanTracker;
import vldb.operator.window.timescale.pafas.*;

/**
 * A helper class for static MTS window configuration.
 */
public final class OntheflyMWOConfiguration extends TimescaleWindowBaseConfiguration {

  public static final ConfigurationModule CONF = new OntheflyMWOConfiguration()
      .merge(TimescaleWindowBaseConfiguration.CONF)
      .bindImplementation(SpanTracker.class, StaticSpanTrackerImpl.class)
      .bindImplementation(TimescaleWindowOperator.class, PafasMWO.class)
      .bindImplementation(DependencyGraph.class, StaticParallelDependencyGraphImpl.class)
      .bindImplementation(DependencyGraph.SelectionAlgorithm.class, OntheflySelectionAlgorithm.class)
      .build();
}
