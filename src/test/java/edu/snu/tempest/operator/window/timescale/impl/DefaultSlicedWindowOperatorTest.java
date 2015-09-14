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

import edu.snu.tempest.operator.OutputEmitter;
import edu.snu.tempest.operator.window.aggregator.CAAggregator;
import edu.snu.tempest.operator.window.aggregator.impl.CountByKeyAggregator;
import edu.snu.tempest.operator.window.aggregator.impl.KeyExtractor;
import edu.snu.tempest.operator.window.timescale.Timescale;
import edu.snu.tempest.operator.window.timescale.parameter.StartTime;
import edu.snu.tempest.test.util.IntegerExtractor;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.mockito.Mockito.*;

public class DefaultSlicedWindowOperatorTest {
  ComputationReuser<Map<Integer, Long>> computationReuser;
  NextSliceTimeProvider nextSliceTimeProvider;
  List<Timescale> timescales;
  IntegerRef counter;
  Injector injector;
  CountByKeyAggregator<Integer, Integer> aggregator;
  Queue<PartialTimeWindowOutput<Map<Integer, Long>>> queue;

  @Before
  public void initialize() throws InjectionException {
    computationReuser = mock(ComputationReuser.class);
    nextSliceTimeProvider = mock(NextSliceTimeProvider.class);
    when(nextSliceTimeProvider.nextSliceTime()).thenReturn(1L, 3L, 4L, 6L, 7L, 9L);
    timescales = new LinkedList<>();
    counter = new IntegerRef(0);
    timescales.add(new Timescale(5, 3));
    queue = new LinkedList<>();
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindImplementation(KeyExtractor.class, IntegerExtractor.class);
    jcb.bindImplementation(CAAggregator.class, CountByKeyAggregator.class);
    jcb.bindNamedParameter(StartTime.class, Long.toString(0));
    injector = Tang.Factory.getTang().newInjector(jcb.build());
    injector.bindVolatileInstance(ComputationReuser.class, computationReuser);
    injector.bindVolatileInstance(NextSliceTimeProvider.class, nextSliceTimeProvider);
  }

  /**
   * SlicedWindowOperator should aggregate the input.
   */
  @Test
  public void defaultSlicedWindowTest() throws InjectionException {
    DefaultSlicedWindowOperator<Integer, Map<Integer, Long>> operator =
        injector.getInstance(DefaultSlicedWindowOperator.class);
    operator.prepare(new OutputEmitter<PartialTimeWindowOutput<Map<Integer, Long>>>() {
      @Override
      public void emit(final PartialTimeWindowOutput<Map<Integer, Long>> output) {
        queue.add(output);
      }
    });
    Map<Integer, Long> result = new HashMap<>();
    result.put(1, 3L); result.put(2, 1L); result.put(3, 1L);
    operator.execute(1); operator.execute(2); operator.execute(3);
    operator.execute(1); operator.execute(1);
    operator.slice(1L);

    final PartialTimeWindowOutput<Map<Integer, Long>> output1 = queue.poll();
    Assert.assertEquals(result, output1.output);
    Assert.assertEquals(output1.windowStartTime, 0);
    Assert.assertEquals(output1.windowEndTime, 1);

    Map<Integer, Long> result2 = new HashMap<>();
    result2.put(1, 2L); result2.put(4, 1L); result2.put(5, 1L); result2.put(3, 1L);
    operator.execute(4); operator.execute(5); operator.execute(3);
    operator.execute(1); operator.execute(1);

    operator.slice(3L);
    final PartialTimeWindowOutput<Map<Integer, Long>> output2 = queue.poll();
    Assert.assertEquals(result2, output2.output);
    Assert.assertEquals(output2.windowStartTime, 1);
    Assert.assertEquals(output2.windowEndTime, 3);

    Map<Integer, Long> result3 = new HashMap<>();
    result3.put(1, 2L); result3.put(4, 1L);
    operator.execute(4); operator.execute(1); operator.execute(1);

    operator.slice(4L);
    final PartialTimeWindowOutput<Map<Integer, Long>> output3 = queue.poll();
    Assert.assertEquals(result3, output3.output);
    Assert.assertEquals(output3.windowStartTime, 3);
    Assert.assertEquals(output3.windowEndTime, 4);

    operator.slice(6L);
    final PartialTimeWindowOutput<Map<Integer, Long>> output4 = queue.poll();
    Assert.assertEquals(new HashMap<Integer, Long>(), output4.output);
    Assert.assertEquals(output4.windowStartTime, 4);
    Assert.assertEquals(output4.windowEndTime, 6);
  }
  
  class IntegerRef {
    public int value;
    
    public IntegerRef(int i) {
      this.value = i;
    }
  }
}
