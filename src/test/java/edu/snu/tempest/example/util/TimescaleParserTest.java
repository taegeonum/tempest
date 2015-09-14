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
package edu.snu.tempest.example.util;

import edu.snu.tempest.operator.window.timescale.Timescale;
import edu.snu.tempest.operator.window.timescale.impl.TimescaleParser;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Test;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public final class TimescaleParserTest {

  /**
   * parse string to timescales.
   * @throws InjectionException
   */
  @Test
  public void parseToTimescaleSuccessTest() throws InjectionException {
    final String str = "(30,2)(40,4)(50,6)(60,7)";
    final List<Timescale> list = new ArrayList<>();
    list.add(new Timescale(30, 2, TimeUnit.SECONDS, TimeUnit.SECONDS));
    list.add(new Timescale(40, 4, TimeUnit.SECONDS, TimeUnit.SECONDS));
    list.add(new Timescale(50, 6, TimeUnit.SECONDS, TimeUnit.SECONDS));
    list.add(new Timescale(60, 7, TimeUnit.SECONDS, TimeUnit.SECONDS));

    final List<Timescale> timescales = TimescaleParser.parseFromString(str);
    assert(timescales.equals(list));
  }

  /**
   * Parse invalid string to timescales.
   * @throws Exception
   */
  @Test(expected=InvalidParameterException.class)
  public void parseToTimescaleInvalidStringTest() {
    final String invalidStr = "23, 45, 15, 12";

    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    TimescaleParser.parseFromString(invalidStr);
  }

  @Test
  public void parseToStringTest() {
    final String str = "(30,2)(40,4)(50,6)(60,7)";
    final List<Timescale> list = new ArrayList<>();
    list.add(new Timescale(30, 2, TimeUnit.SECONDS, TimeUnit.SECONDS));
    list.add(new Timescale(40, 4, TimeUnit.SECONDS, TimeUnit.SECONDS));
    list.add(new Timescale(50, 6, TimeUnit.SECONDS, TimeUnit.SECONDS));
    list.add(new Timescale(60, 7, TimeUnit.SECONDS, TimeUnit.SECONDS));
    assert(str.compareTo(TimescaleParser.parseToString(list)) == 0);
  }
}
