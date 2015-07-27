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
package edu.snu.tempest.operators.common;

import edu.snu.tempest.operators.Timescale;
import edu.snu.tempest.operators.common.impl.DefaultMTSOperatorSchedulerImpl;
import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.wake.Stage;

/**
 * Scheduler for MTS operator.
 * It schedules the execution of SlicedWindowOperator and OverlappingWindowOperators
 * according to the order.
 */
@DefaultImplementation(DefaultMTSOperatorSchedulerImpl.class)
public interface MTSOperatorScheduler extends Stage {
  /**
   * Start the scheduler.
   */
  void start();
  
  /**
   * Subscribe an overlapping window operator and return a subscription for unsubscribe.
   * @param overlappingWindowoperator an overlapping window operator.
   * @return a subscription for the overlapping window operator.
   */
  Subscription<Timescale> subscribe(final OverlappingWindowOperator<?> overlappingWindowoperator);
}
