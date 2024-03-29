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
package edu.snu.tempest.operator.window.aggregator.impl;

import edu.snu.tempest.test.util.IntegerExtractor;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public final class CountByKeyAggregatorTest {

  /**
   * Test CountByKeyAggregator.
   */
  @Test
  public void aggregationTest() throws InjectionException {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindImplementation(KeyExtractor.class, IntegerExtractor.class);
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    final CountByKeyAggregator<Integer, Integer> aggregator =
        injector.getInstance(CountByKeyAggregator.class);

    Map<Integer, Long> map1 = aggregator.init();
    aggregator.incrementalAggregate(map1, 1);
    aggregator.incrementalAggregate(map1, 2);
    aggregator.incrementalAggregate(map1, 2);
    aggregator.incrementalAggregate(map1, 2);
    aggregator.incrementalAggregate(map1, 3);
    aggregator.incrementalAggregate(map1, 1);

    Map<Integer, Long> map2 = aggregator.init();
    aggregator.incrementalAggregate(map2, 5);
    aggregator.incrementalAggregate(map2, 2);
    aggregator.incrementalAggregate(map2, 2);
    aggregator.incrementalAggregate(map2, 1);
    aggregator.incrementalAggregate(map2, 3);
    aggregator.incrementalAggregate(map2, 5);

    final List<Map<Integer, Long>> partials = new LinkedList<>();
    partials.add(map1);
    partials.add(map2);

    final Map<Integer, Long> result = aggregator.aggregate(partials);

    final Map<Integer, Long> expected = new HashMap<>();
    expected.put(1, 3L);
    expected.put(2, 5L);
    expected.put(3, 2L);
    expected.put(5, 2L);
    Assert.assertEquals(expected, result);
  }
}
