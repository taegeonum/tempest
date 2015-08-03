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
import edu.snu.tempest.util.test.TestAggregator;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.*;

public class StaticSlicedWindowOperatorTest {
  StaticComputationReuser<Map<Integer, Integer>> computationReuser;
  List<Timescale> timescales;
  IntegerRef counter;
  TestAggregator aggregator;
  
  @Before
  public void initialize() throws InjectionException {
    computationReuser = mock(StaticComputationReuser.class);
    timescales = new LinkedList<>();
    counter = new IntegerRef(0);
    timescales.add(new Timescale(5, 3));
    aggregator = Tang.Factory.getTang().newInjector().getInstance(TestAggregator.class);
  }

  /**
   * SlicedWindowOperator should aggregate the input.
   */
  @Test
  public void defaultSlicedWindowTest() {
    final StaticSlicedWindowOperatorImpl<Integer, Map<Integer, Integer>> operator =
        new StaticSlicedWindowOperatorImpl<>(aggregator, computationReuser, 0L);

    when(computationReuser.nextSliceTime()).thenReturn(1L, 3L, 4L, 6L, 7L, 9L, 10L, 12L);

    final Map<Integer, Integer> result = new HashMap<>();
    result.put(1, 3); result.put(2, 1); result.put(3, 1);
    operator.execute(1); operator.execute(2); operator.execute(3);
    operator.execute(1); operator.execute(1);
    operator.onNext(1L);
    verify(computationReuser).savePartialOutput(0, 1, result);

    final Map<Integer, Integer> result2 = new HashMap<>();
    result2.put(1, 2); result2.put(4, 1); result2.put(5, 1); result2.put(3, 1);
    operator.execute(4); operator.execute(5); operator.execute(3);
    operator.execute(1); operator.execute(1);

    operator.onNext(3L);
    verify(computationReuser).savePartialOutput(1, 3, result2);

    final Map<Integer, Integer> result3 = new HashMap<>();
    result3.put(1, 2); result3.put(4, 1);
    operator.execute(4); operator.execute(1); operator.execute(1);

    operator.onNext(4L);
    verify(computationReuser).savePartialOutput(3, 4, result3);

    operator.onNext(6L);
    verify(computationReuser).savePartialOutput(4, 6, new HashMap<Integer, Integer>());
  }
  
  class IntegerRef {
    public int value;
    
    public IntegerRef(int i) {
      this.value = i;
    }
  }
}
