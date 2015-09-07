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
package edu.snu.tempest.operator.filter;

import edu.snu.tempest.operator.OutputEmitter;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public final class FilterOperatorTest {

  /**
   * Test filter operator.
   * It filters string values which start with "a".
   */
  @Test
  public void testFilterOperator() throws InjectionException {
    final String[] inputs = {"alpha", "bravo", "charlie", "delta", "application", "echo", "ally", "foxtrot"};
    final String[] expectedOutput = {"bravo", "charlie", "delta", "echo", "foxtrot"};
    final List<String> expected = Arrays.asList(expectedOutput);
    final List<String> result = new LinkedList<>();

    final Injector injector = Tang.Factory.getTang().newInjector();
    final TestFilterFunction filterFunc = new TestFilterFunction();
    injector.bindVolatileInstance(FilterFunction.class, filterFunc);
    final FilterOperator<String> filterOperator = injector.getInstance(FilterOperator.class);
    filterOperator.prepare(new OutputEmitter<String>() {
      @Override
      public void emit(final String output) {
        result.add(output);
      }
    });

    for (final String input : inputs) {
      filterOperator.execute(input);
    }
    Assert.assertEquals(expected, result);
  }

  class TestFilterFunction implements FilterFunction<String> {

    @Override
    public boolean filter(final String input) {
      return input.startsWith("a");
    }
  }
}
