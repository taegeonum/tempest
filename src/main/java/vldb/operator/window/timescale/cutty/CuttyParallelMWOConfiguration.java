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
package vldb.operator.window.timescale.cutty;

import org.apache.reef.tang.formats.ConfigurationModule;
import vldb.operator.window.timescale.TimescaleWindowBaseConfiguration;
import vldb.operator.window.timescale.TimescaleWindowOperator;

/**
 * A helper class for static MTS window configuration.
 */
public final class CuttyParallelMWOConfiguration extends TimescaleWindowBaseConfiguration {

    public static final ConfigurationModule CONF = new CuttyParallelMWOConfiguration()
        .merge(TimescaleWindowBaseConfiguration.CONF)
        .bindImplementation(TimescaleWindowOperator.class, CuttyParallelMWO.class)
        .bindImplementation(Fat.class, CuttyFlatFat.class)
        .build();
}
