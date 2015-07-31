/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package edu.snu.tempest.operator.window.mts;

import edu.snu.tempest.operator.window.Timescale;
import edu.snu.tempest.operator.window.mts.impl.CachingRatePolicy;
import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * CachingPolicy for caching multi-time scale outputs.
 */
@DefaultImplementation(CachingRatePolicy.class)
public interface CachingPolicy {
  /**
   * Decide to cache or not the output of ts ranging from startTime to endTime.
   * @param startTime start time of the output
   * @param endTime end time of the output
   * @param ts the timescale of the output
   * @return cache or not
   */
  boolean cache(long startTime, long endTime, Timescale ts);

  /**
   * Receive timescale to be added.
   * @param ts timescale to be added.
   * @param startTime the time when timescale is added.
   */
  void addTimescale(Timescale ts, final long startTime);

  /**
   * Receive timescale to be deleted.
   * @param ts timescale to be deleted.
   */
  void removeTimescale(Timescale ts);
}