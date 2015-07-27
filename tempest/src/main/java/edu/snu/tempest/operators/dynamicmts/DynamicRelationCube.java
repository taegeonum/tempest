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
package edu.snu.tempest.operators.dynamicmts;

import edu.snu.tempest.operators.Timescale;
import edu.snu.tempest.operators.common.RelationCube;
import edu.snu.tempest.operators.dynamicmts.impl.DynamicRelationCubeImpl;
import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.wake.EventHandler;

/**
 * Dynamic RelationCube.
 * It saves final aggregation results according to caching rate.
 *
 */
@DefaultImplementation(DynamicRelationCubeImpl.class)
public interface DynamicRelationCube<T> extends RelationCube<T>, TimescaleSignalListener {
  /**
   * GarbageCollector interface.
   *
   * It periodically removes outputs saved in OutputLookupTable.
   * It receives LogicalTime tick from Clock
   */
  public interface GarbageCollector extends TimescaleSignalListener, EventHandler<Long> {

  }

  /**
   * CachingPolicy for caching multi-time scale outputs.
   */
  public interface CachingPolicy extends TimescaleSignalListener {
    /**
     * Decide to cache or not the output of ts ranging from startTime to endTime.
     * @param startTime start time of the output
     * @param endTime end time of the output
     * @param ts the timescale of the output
     * @return cache or not
     */
    boolean cache(long startTime, long endTime, Timescale ts);
  }
}
