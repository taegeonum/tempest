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
import vldb.operator.window.timescale.common.*;
import vldb.operator.window.timescale.pafas.*;
import vldb.operator.window.timescale.pafas.dynamic.*;
import vldb.operator.window.timescale.pafas.vldb2018.singlethread.SingleThreadFinalAggregator;

/**
 * A helper class for static MTS window configuration.
 */
public final class OntheflyMWOConfiguration extends TimescaleWindowBaseConfiguration {

  public static final ConfigurationModule CONF = new OntheflyMWOConfiguration()
      .merge(TimescaleWindowBaseConfiguration.CONF)
      .bindImplementation(DynamicOutputLookupTable.class, DynamicDPOutputLookupTableImpl.class)
      .bindImplementation(SpanTracker.class, DynamicSpanTrackerImpl.class)
      .bindImplementation(TimescaleWindowOperator.class, DynamicMWO.class)
      .bindImplementation(DependencyGraph.SelectionAlgorithm.class, OntheflySelectionAlgorithm.class)
      .bindImplementation(DynamicPartialTimespans.class, DynamicOptimizedPartialTimespans.class)
      .bindImplementation(PartialTimespans.class, DynamicOptimizedPartialTimespans.class)
      .bindImplementation(FinalAggregator.class, SingleThreadFinalAggregator.class)
      .bindImplementation(DynamicDependencyGraph.class, DynamicSmallCostAddDependencyGraphImpl.class)
      .build();

public static final ConfigurationModule STATIC_CONF = new OntheflyMWOConfiguration()
    .merge(TimescaleWindowBaseConfiguration.CONF)
    .bindImplementation(DependencyGraph.SelectionAlgorithm.class, OntheflyStaticSelectionAlgorithm.class)
    .bindImplementation(OutputLookupTable.class, DPOutputLookupTableImpl.class)
    .bindImplementation(SpanTracker.class, StaticSpanTrackerImpl.class)
    .bindImplementation(TimescaleWindowOperator.class, PafasMWO.class)
    .bindImplementation(PartialAggregator.class, DefaultPartialAggregator.class)
    .bindImplementation(PartialTimespans.class, DefaultPartialTimespans.class)
    .bindImplementation(FinalAggregator.class, SingleThreadFinalAggregator.class)
    .bindImplementation(DependencyGraph.class, StaticDependencyGraphImpl.class)
    .build();
}
