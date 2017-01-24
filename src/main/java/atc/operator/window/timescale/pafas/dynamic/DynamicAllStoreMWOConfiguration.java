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
package atc.operator.window.timescale.pafas.dynamic;

import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.RequiredImpl;
import atc.operator.window.timescale.TimescaleWindowBaseConfiguration;
import atc.operator.window.timescale.TimescaleWindowOperator;
import atc.operator.window.timescale.common.FinalAggregator;
import atc.operator.window.timescale.common.SingleThreadFinalAggregator;
import atc.operator.window.timescale.common.SpanTracker;
import atc.operator.window.timescale.pafas.DependencyGraph;

/**
 * A helper class for static MTS window configuration.
 */
public final class DynamicAllStoreMWOConfiguration extends TimescaleWindowBaseConfiguration {

    public static final RequiredImpl<DependencyGraph.SelectionAlgorithm> SELECTION_ALGORITHM = new RequiredImpl<>();
    public static final RequiredImpl<DynamicOutputLookupTable> OUTPUT_LOOKUP_TABLE = new RequiredImpl<>();
    public static final RequiredImpl<DynamicPartialTimespans> DYNAMIC_PARTIAL = new RequiredImpl<>();
    public static final RequiredImpl<DynamicDependencyGraph> DYNAMIC_DEPENDENCY = new RequiredImpl<>();

    public static final ConfigurationModule CONF = new DynamicAllStoreMWOConfiguration()
        .merge(TimescaleWindowBaseConfiguration.CONF)
        .bindImplementation(DependencyGraph.SelectionAlgorithm.class, SELECTION_ALGORITHM)
        .bindImplementation(DynamicOutputLookupTable.class, OUTPUT_LOOKUP_TABLE)
        .bindImplementation(DynamicDependencyGraph.class, DYNAMIC_DEPENDENCY)
        .bindImplementation(DynamicPartialTimespans.class, DYNAMIC_PARTIAL)
        .bindImplementation(SpanTracker.class, DynamicAllStoreSpanTrackerImpl.class)
        .bindImplementation(TimescaleWindowOperator.class, DynamicMWO.class)
        .bindImplementation(FinalAggregator.class, SingleThreadFinalAggregator.class)
        .build();
}
