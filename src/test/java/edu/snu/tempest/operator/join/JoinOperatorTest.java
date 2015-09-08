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
package edu.snu.tempest.operator.join;

import edu.snu.tempest.operator.OutputEmitter;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public final class JoinOperatorTest {

  private static final String STRING_INPUT_IDENTIFIER = "string_input";
  private static final String INTEGER_INPUT_IDENTIFIER = "number";

  /**
   * Test join operator.
   * It joins string and integer value, and repeat the string by the integer value times.
   * @throws InjectionException
   */
  @Test
  public void testJoinOperator() throws InjectionException {
    final String[] keys = {"a", "b", "c", "d", "e", "a", "b", "c", "d", "e"};
    final int[] intVals = {1, 2, 3, 4, 5, 5, 4, 3, 2, 1};
    final List<String> outputs = new LinkedList<>();
    final List<String> expectedJoinOutputs = new LinkedList<>();

    for (int i = 0; i < 10; i++) {
      expectedJoinOutputs.add(stringRepeat(keys[i], intVals[i]));
    }

    final Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileInstance(JoinFunction.class, new TestJoinFunction());
    injector.bindVolatileInstance(JoinCondition.class, new TestJoinCondition());
    final JoinOperator<String, String> joinOperator = injector.getInstance(JoinOperator.class);
    joinOperator.prepare(new OutputEmitter<String>() {
      @Override
      public void emit(final String output) {
        outputs.add(output);
      }
    });

    for (int i = 0; i < 10; i++) {
      joinOperator.execute(new TestJoinInput<>(STRING_INPUT_IDENTIFIER, keys[i], keys[i]));
      joinOperator.execute(new TestJoinInput<>(INTEGER_INPUT_IDENTIFIER, keys[i], intVals[i]));
    }

    int i = 0;
    for (final String output : outputs) {
      System.out.println(output);
      Assert.assertEquals(expectedJoinOutputs.get(i), output);
      i++;
    }
  }

  private String stringRepeat(final String string, final int repeat) {
    final StringBuilder sb = new StringBuilder();
    for (int i = 0; i < repeat; i++) {
      sb.append(string);
    }
    return sb.toString();
  }

  /**
   * Join input.
   */
  class TestJoinInput<V> implements JoinInput<String> {

    private final String identifier;
    private final String key;
    private final V input;

    public TestJoinInput(final String identifier,
                         final String key,
                         final V input) {
      this.identifier = identifier;
      this.key = key;
      this.input = input;
    }

    @Override
    public String getIdentifier() {
      return identifier;
    }

    @Override
    public String getJoinKey() {
      return key;
    }

    @Override
    public V getValue() {
      return input;
    }
  }

  /**
   * Join the string and integer
   * and repeat the string.
   */
  class TestJoinFunction implements JoinFunction<String, String> {
    @Override
    public String join(final String key, final Collection<IdentifierAndValue> identifierAndValues) {
      String stringInput = null;
      int repeat = 0;
      for (final IdentifierAndValue identifierAndValue : identifierAndValues) {
        if (identifierAndValue.identifier.equals("string_input")) {
          stringInput = (String) identifierAndValue.value;
        } else if (identifierAndValue.identifier.equals("number")) {
          repeat = (Integer) identifierAndValue.value;
        }
      }
      return stringRepeat(stringInput, repeat);
    }
  }

  class TestJoinCondition implements JoinCondition<String> {
    private final Map<String, Integer> keyAndCount = new HashMap<>();

    @Override
    public boolean readyToJoin(final String key, final IdentifierAndValue joinInput) {
      Integer count = keyAndCount.get(key);
      if (count == null) {
        count = 1;
      } else {
        count++;
      }
      keyAndCount.put(key, count);
      return count == 2;
    }

    @Override
    public void reset(final String key) {
      keyAndCount.put(key, 0);
    }
  }
}
