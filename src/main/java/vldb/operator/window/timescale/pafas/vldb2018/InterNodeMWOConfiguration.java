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
package vldb.operator.window.timescale.pafas.vldb2018;

import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.RequiredImpl;
import vldb.operator.window.timescale.TimescaleWindowBaseConfiguration;
import vldb.operator.window.timescale.TimescaleWindowOperator;
import vldb.operator.window.timescale.common.*;
import vldb.operator.window.timescale.pafas.*;

/**
 * A helper class for static MTS window configuration.
 */
public final class InterNodeMWOConfiguration extends TimescaleWindowBaseConfiguration {

    public static final RequiredImpl<OutputLookupTable> OUTPUT_LOOKUP_TABLE = new RequiredImpl<>();
    public static final RequiredImpl<DependencyGraph> DEPENDENCY_GRAPH = new RequiredImpl<>();

    public static final RequiredImpl<DependencyGraph.SelectionAlgorithm> SELECTION_ALGORITHM = new RequiredImpl<>();
    public static final ConfigurationModule CONF = new InterNodeMWOConfiguration()
        .merge(TimescaleWindowBaseConfiguration.CONF)
        .bindImplementation(DependencyGraph.SelectionAlgorithm.class, SELECTION_ALGORITHM)
        .bindImplementation(OutputLookupTable.class, OUTPUT_LOOKUP_TABLE)
        .bindImplementation(SpanTracker.class, StaticSpanTrackerImpl.class)
        .bindImplementation(TimescaleWindowOperator.class, PafasMWO.class)
        .bindImplementation(PartialAggregator.class, DefaultPartialAggregator.class)
        .bindImplementation(PartialTimespans.class, SizeOnePartialTimespans.class)
        .bindImplementation(FinalAggregator.class, SingleThreadFinalAggregator.class)
        .bindImplementation(DependencyGraph.class, DEPENDENCY_GRAPH)
        .build();
}
