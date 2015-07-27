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
package edu.snu.tempest.examples.utils;

import edu.snu.tempest.operators.Timescale;
import org.apache.reef.tang.Injector;
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
  public void parseSuccessTest() throws InjectionException {
    final String str = "(30,2)(40,4)(50,6)(60,7)";
    final List<Timescale> list = new ArrayList<>();
    list.add(new Timescale(30, 2, TimeUnit.SECONDS, TimeUnit.SECONDS));
    list.add(new Timescale(40, 4, TimeUnit.SECONDS, TimeUnit.SECONDS));
    list.add(new Timescale(50, 6, TimeUnit.SECONDS, TimeUnit.SECONDS));
    list.add(new Timescale(60, 7, TimeUnit.SECONDS, TimeUnit.SECONDS));

    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.bindNamedParameter(TimescaleParser.TimescaleParameter.class, str);
    final Injector ij = Tang.Factory.getTang().newInjector(cb.build());

    final TimescaleParser tsc = ij.getInstance(TimescaleParser.class);
    final List<Timescale> timescales = tsc.timescales;
    
    assert(timescales.equals(list));
  }

  /**
   * Parse invalid string to timescales.
   * @throws Exception
   */
  @Test
  public void parseInvalidStringTest() throws Exception {
    final String invalidStr = "23, 45, 15, 12";

    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.bindNamedParameter(TimescaleParser.TimescaleParameter.class, invalidStr);
    final Injector ij = Tang.Factory.getTang().newInjector(cb.build());

    TimescaleParser tsc = null;
    try {
      tsc = ij.getInstance(TimescaleParser.class);
    } catch (InjectionException e) {
      assert(e.getCause().getClass().equals(InvalidParameterException.class));
    }
    
    if (tsc != null) { 
      throw new Exception("Timescale parser should throw InvalidParameterException");
    }
  }
}
