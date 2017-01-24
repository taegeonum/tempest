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
package atc.operator.window.timescale.pafas.infinite;

import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.RequiredImpl;
import atc.operator.window.timescale.TimescaleWindowBaseConfiguration;
import atc.operator.window.timescale.TimescaleWindowOperator;
import atc.operator.window.timescale.common.FinalAggregator;
import atc.operator.window.timescale.common.SingleThreadFinalAggregator;
import atc.operator.window.timescale.common.SpanTracker;
import atc.operator.window.timescale.pafas.DependencyGraph;
import atc.operator.window.timescale.pafas.PafasMWO;
import atc.operator.window.timescale.pafas.PartialTimespans;
import atc.operator.window.timescale.pafas.StaticSpanTrackerImpl;

/**
 * A helper class for static MTS window configuration.
 */
public final class InfiniteMWOConfiguration extends TimescaleWindowBaseConfiguration {

    public static final RequiredImpl<DependencyGraph.SelectionAlgorithm> SELECTION_ALGORITHM = new RequiredImpl<>();

    public static final ConfigurationModule CONF = new InfiniteMWOConfiguration()
        .merge(TimescaleWindowBaseConfiguration.CONF)
        .bindImplementation(DependencyGraph.SelectionAlgorithm.class, SELECTION_ALGORITHM)
        .bindImplementation(SpanTracker.class, StaticSpanTrackerImpl.class)
        .bindImplementation(TimescaleWindowOperator.class, PafasMWO.class)
        .bindImplementation(PartialTimespans.class, InfinitePartialTimespans.class)
        .bindImplementation(FinalAggregator.class, SingleThreadFinalAggregator.class)
        .bindImplementation(DependencyGraph.class, InfiniteDependencyGraphImpl.class)
        .build();
}
