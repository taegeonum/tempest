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
package edu.snu.tempest.operator.window.time.common;

import edu.snu.tempest.operator.window.time.Timescale;
import org.apache.reef.wake.EventHandler;


/**
 * OverlappingWindowOperator.
 *
 * It is triggered and does final aggregation per interval.
 * For example, if a timescale is [w=10s, i=3s],
 * then OWO produces an output with 10 seconds window size every 3 seconds.
 */
public interface OverlappingWindowOperator<V> extends EventHandler<Long> {
  /**
   * Return a timescale related to this overlapping window operator.
   * @return timescale.
   */
  Timescale getTimescale();
}