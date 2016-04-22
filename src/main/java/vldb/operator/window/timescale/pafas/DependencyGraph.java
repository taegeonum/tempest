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

package vldb.operator.window.timescale.pafas;

import vldb.operator.window.timescale.Timescale;
import vldb.operator.window.timescale.common.Timespan;

import java.util.List;

public interface DependencyGraph<T> {

  /**
   * Get final timespans triggered at time t.
   * @param t time to trigger final timespans
   * @return a list of final timespans
   */
  List<Timespan> getFinalTimespans(final long t);

  Node<T> getNode(Timespan timespan);

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


  public interface SelectionAlgorithm<T> {
    /**
     * Find child nodes which are included in the timespan of [start-end]
     * from partialTimespans and finalTimespans.
     * For example, if the range is [0-10]
     * it finds dependency graph nodes which are included in the range of [0-10].
     * @param start start time of the timespan
     * @param end end time of the timespan.
     * @return child nodes
     */
    List<Node<T>> selection(long start, long end);
  }
}
