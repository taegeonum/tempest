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
package atc.operator.window.timescale.common;

import atc.operator.window.timescale.Timescale;
import atc.operator.window.timescale.pafas.Node;

import java.util.List;

/**
 * SpanTracker for MWO.
 * This tracks dependent timespans for final timespans.
 */
public interface SpanTracker<T> {

  /**
   * Get a next partial timespan after time t.
   * @return {timespan}
   */
  long getNextSliceTime(final long t);

  /**
   * Get final timespans triggered at time t.
   * @param t time to trigger final timespans
   * @return a list of final timespans
   */
  List<Timespan> getFinalTimespans(final long t);

  void putAggregate(final T agg, Timespan timespan);

  List<T> getDependentAggregates(final Timespan timespan);

  List<Node<T>> getDependentNodes(final Timespan timespan);
  /**
   * Receive timescale to be added.
   * @param ts timescale to be added.
   * @param addTime the time when timescale is added.
   */
  void addSlidingWindow(Timescale ts, long addTime);

  /**
   * Receive timescale to be deleted.
   * @param ts timescale to be deleted.
   * @param deleteTime the time when timescale is deleted.
   */
  void removeSlidingWindow(Timescale ts, long deleteTime);
}
