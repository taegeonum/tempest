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
    injector.bindVolatileInstance(JoinFunc.class, new TestJoinFunc());
    injector.bindVolatileInstance(JoinCondition.class, new TestJoinCondition());
    final JoinOperator<String, String> joinOperator = injector.getInstance(JoinOperator.class);
    joinOperator.prepare(new OutputEmitter<String>() {
      @Override
      public void emit(final String output) {
        outputs.add(output);
      }
    });

    for (int i = 0; i < 10; i++) {
      joinOperator.execute(new TestStringJoinInput(keys[i], keys[i]));
      joinOperator.execute(new TestIntegerJoinInput(keys[i], intVals[i]));
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
   * String input.
   */
  class TestStringJoinInput implements JoinInput<String> {

    private final String key;
    private final String input;

    public TestStringJoinInput(final String key,
                         final String input) {
      this.key = key;
      this.input = input;
    }

    @Override
    public String getIdentifier() {
      return "string_input";
    }

    @Override
    public String getJoinKey() {
      return key;
    }

    @Override
    public String getValue() {
      return input;
    }
  }

  /**
   * Repeat number input.
   */
  class TestIntegerJoinInput implements JoinInput<String> {

    private final String key;
    private final int input;

    public TestIntegerJoinInput(final String key,
                                final int input) {
      this.key = key;
      this.input = input;
    }

    @Override
    public String getIdentifier() {
      return "number";
    }

    @Override
    public String getJoinKey() {
      return key;
    }

    @Override
    public Integer getValue() {
      return input;
    }
  }


  /**
   * Join the string and integer
   * and repeat the string.
   */
  class TestJoinFunc implements JoinFunc<String, String> {
    @Override
    public String join(final String key, final Collection<IdentifierAndValue> joinInputs) {
      String stringInput = null;
      int repeat = 0;
      for (final IdentifierAndValue joinInput : joinInputs) {
        if (joinInput.identifier.equals("string_input")) {
          stringInput = (String)joinInput.value;
        } else if (joinInput.identifier.equals("number")) {
          repeat = (Integer)joinInput.value;
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
