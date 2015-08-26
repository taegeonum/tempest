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
package edu.snu.tempest.operator.map;

import edu.snu.tempest.operator.OutputEmitter;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public final class MapOperatorTest {

  /**
   * Test map operator.
   * It splits string value and returns the list of string.
   */
  @Test
  public void testMapOperator() throws InjectionException {
    final String input = "a b c d e f g";
    final String[] split = {"a", "b", "c", "d", "e", "f", "g"};
    final List<String> expected = Arrays.asList(split);
    final AtomicReference<List<String>> result = new AtomicReference<>();

    final Injector injector = Tang.Factory.getTang().newInjector();
    final TestSplitMapFunc mapFunc = new TestSplitMapFunc();
    injector.bindVolatileInstance(MapFunc.class, mapFunc);
    final MapOperator<String, List<String>> mapOperator = injector.getInstance(MapOperator.class);
    mapOperator.prepare(new OutputEmitter<List<String>>() {
      @Override
      public void emit(final List<String> output) {
        result.set(output);
      }
    });

    mapOperator.execute(input);
    Assert.assertEquals(expected, result.get());
  }

  class TestSplitMapFunc implements MapFunc<String, List<String>> {

    @Override
    public List<String> map(final String input) {
      final String[] split = input.split(" ");
      return Arrays.asList(split);
    }
  }
}
