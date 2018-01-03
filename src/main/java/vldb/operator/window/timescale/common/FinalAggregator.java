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
package vldb.operator.window.timescale.common;

import org.apache.reef.tang.annotations.DefaultImplementation;
import vldb.operator.window.timescale.pafas.Node;

import java.util.List;
import java.util.Map;

/**
 * FinalAggregator does final aggregates.
 */
@DefaultImplementation(DefaultFinalAggregator.class)
public interface FinalAggregator<V> extends AutoCloseable {

  /**
   * Trigger final aggregations of the final timespans.
   * @param finalTimespans final timespans
   */
  void triggerFinalAggregation(List<Timespan> finalTimespans, long actualTriggerTime);

  void triggerFinalAggregation(Map<Long, Node<V>> nodes, long endTime, long actualTriggerTime);
}