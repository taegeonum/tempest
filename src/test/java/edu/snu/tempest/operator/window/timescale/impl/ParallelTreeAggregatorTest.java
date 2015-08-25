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

import edu.snu.tempest.operator.window.aggregator.CAAggregator;
import edu.snu.tempest.operator.window.aggregator.impl.CountByKeyAggregator;
import edu.snu.tempest.operator.window.aggregator.impl.KeyExtractor;
import edu.snu.tempest.test.util.IntegerExtractor;
import edu.snu.tempest.test.util.MTSTestUtils;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public final class ParallelTreeAggregatorTest {

  /**
   * Test parallel tree aggregation.
   * It aggregates 20 partial outputs by parallelizing the aggregation.
   */
  @Test
  public void parallelAggregationTest() throws InjectionException {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindImplementation(KeyExtractor.class, IntegerExtractor.class);
    jcb.bindImplementation(CAAggregator.class, CountByKeyAggregator.class);

    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    final CountByKeyAggregator<Integer, Integer> aggregator = injector.getInstance(CountByKeyAggregator.class);
    final List<Map<Integer, Long>> partialOutputs = new LinkedList<>();

    final Random rand = new Random();
    for (int i = 0; i < 20; i++) {
      final Map<Integer, Long> partialOutput = new HashMap<>();
      for (int j = 0; j < 100; j++) {
        partialOutput.put(rand.nextInt(), rand.nextLong());
      }
      partialOutputs.add(partialOutput);
    }

    final ParallelTreeAggregator<Integer, Map<Integer, Long>> parallelTreeAggregator =
        new ParallelTreeAggregator<>(8, 8, aggregator);
    final Map<Integer, Long> result = parallelTreeAggregator.doParallelAggregation(partialOutputs);
    Assert.assertEquals(MTSTestUtils.merge(partialOutputs), result);
  }
}
