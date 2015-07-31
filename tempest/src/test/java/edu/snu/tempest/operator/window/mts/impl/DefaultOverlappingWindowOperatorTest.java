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
package edu.snu.tempest.operator.window.mts.impl;

import edu.snu.tempest.operator.common.NotFoundException;
import edu.snu.tempest.operator.window.Timescale;
import edu.snu.tempest.operator.window.common.OverlappingWindowOperator;
import edu.snu.tempest.operator.window.common.TSOutputGenerator;
import edu.snu.tempest.operator.window.mts.MTSWindowOperator;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public final class DefaultOverlappingWindowOperatorTest {

  /**
   * Overlapping window operator should call tsOutputGenerator.finalAggregate
   * every its interval.
   */
  @Test
  public void overlappingWindowOperatorTest() throws NotFoundException {
    final Map<Integer, Integer> map = new HashMap<>();
    map.put(1, 1);
    final Timescale ts = new Timescale(5, 3);
    final TSOutputGenerator<Map<Integer, Integer>> cube = mock(TSOutputGenerator.class);
    final MTSWindowOperator.MTSOutputHandler<Map<Integer, Integer>> outputHandler =
        mock(MTSWindowOperator.MTSOutputHandler.class);
    final OverlappingWindowOperator<Map<Integer, Integer>> operator = new DefaultOverlappingWindowOperator<>(
        ts, cube, outputHandler, 0L);
    operator.onNext(3L);
    verify(cube).finalAggregate(-2, 3, ts);
    operator.onNext(6L);
    verify(cube).finalAggregate(1, 6, ts);
  }
}
