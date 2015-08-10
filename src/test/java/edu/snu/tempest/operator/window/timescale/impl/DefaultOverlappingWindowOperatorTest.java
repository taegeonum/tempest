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
package edu.snu.tempest.operator.window.timescale.impl;

import edu.snu.tempest.operator.common.NotFoundException;
import edu.snu.tempest.operator.window.timescale.TimescaleWindowOutputHandler;
import edu.snu.tempest.operator.window.timescale.Timescale;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public final class DefaultOverlappingWindowOperatorTest {

  /**
   * Overlapping window operator should call computationReuser.finalAggregate
   * every its interval.
   */
  @Test
  public void overlappingWindowOperatorTest() throws NotFoundException {
    final Map<Integer, Integer> map = new HashMap<>();
    map.put(1, 1);
    final Timescale ts = new Timescale(5, 3);
    final ComputationReuser<Map<Integer, Integer>> computationReuser = mock(ComputationReuser.class);
    final TimescaleWindowOutputHandler<Map<Integer, Integer>> outputHandler =
        mock(TimescaleWindowOutputHandler.class);
    final OverlappingWindowOperator operator = new DefaultOverlappingWindowOperator<>(
        ts, computationReuser, outputHandler, 0L);
    operator.onNext(3L);
    verify(computationReuser).finalAggregate(-2, 3, ts);
    operator.onNext(6L);
    verify(computationReuser).finalAggregate(1, 6, ts);
  }
}
