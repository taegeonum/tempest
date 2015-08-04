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
package edu.snu.tempest.operator.window.time.impl;

import edu.snu.tempest.operator.window.time.Timescale;
import edu.snu.tempest.operator.window.time.TimescaleParser;
import edu.snu.tempest.operator.window.time.parameter.StartTime;
import edu.snu.tempest.operator.window.time.parameter.TimescaleString;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

public final class NextSliceProviderTest {

  List<Timescale> timescales;
  Timescale ts1;
  Timescale ts2;
  Timescale ts3;
  long startTime;

  @Before
  public void initialize() {
    timescales = new LinkedList<>();
    ts1 = new Timescale(4, 2);
    ts2 = new Timescale(6, 3);
    ts3 = new Timescale(10, 4);
    timescales.add(ts1); timescales.add(ts2); timescales.add(ts3);
    startTime = 0;
  }

  @Test
  public void staticNextSlicTimeTest() throws InjectionException {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(TimescaleString.class, TimescaleParser.parseToString(timescales));
    jcb.bindImplementation(NextSliceTimeProvider.class, StaticNextSliceTimeProvider.class);
    jcb.bindNamedParameter(StartTime.class, Integer.toString(0));
    nextSliceTimeTest(jcb.build());
  }

  /**
   * Test static next slice time.
   */
  public void nextSliceTimeTest(Configuration conf) throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector(conf);
    final NextSliceTimeProvider sliceTimeProvider = injector.getInstance(NextSliceTimeProvider.class);

    Assert.assertEquals(2L, sliceTimeProvider.nextSliceTime());
    Assert.assertEquals(3L, sliceTimeProvider.nextSliceTime());
    Assert.assertEquals(4L, sliceTimeProvider.nextSliceTime());
    Assert.assertEquals(6L, sliceTimeProvider.nextSliceTime());
    Assert.assertEquals(8L, sliceTimeProvider.nextSliceTime());
    Assert.assertEquals(9L, sliceTimeProvider.nextSliceTime());
    Assert.assertEquals(10L, sliceTimeProvider.nextSliceTime());
    Assert.assertEquals(12L, sliceTimeProvider.nextSliceTime());
    Assert.assertEquals(14L, sliceTimeProvider.nextSliceTime());
    Assert.assertEquals(15L, sliceTimeProvider.nextSliceTime());
    Assert.assertEquals(16L, sliceTimeProvider.nextSliceTime());
    Assert.assertEquals(18L, sliceTimeProvider.nextSliceTime());
  }
}
