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
package edu.snu.tempest.operator.window.time.mts.impl;


import edu.snu.tempest.operator.window.aggregator.ComAndAscAggregator;
import edu.snu.tempest.operator.window.aggregator.impl.CountByKeyAggregator;
import edu.snu.tempest.operator.window.aggregator.impl.KeyExtractor;
import edu.snu.tempest.operator.window.time.Timescale;
import edu.snu.tempest.operator.window.time.mts.MTSWindowOperator;
import edu.snu.tempest.operator.window.time.mts.MTSWindowOutput;
import edu.snu.tempest.operator.window.time.mts.parameters.InitialStartTime;
import edu.snu.tempest.util.test.IntegerExtractor;
import edu.snu.tempest.util.test.MTSTestUtils;
import edu.snu.tempest.util.test.Monitor;
import edu.snu.tempest.util.test.TestOutputHandler;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class StaticMTSOperatorTest {

  /**
   * Aggregates multi-time scale outputs.
   */
  @Test
  public void multipleTimescaleAggregationTest() throws Exception {
    final ExecutorService executor = Executors.newFixedThreadPool(1);
    final Monitor monitor = new Monitor();
    final Boolean finished = new Boolean(false);
    final Random random = new Random();
    final List<Timescale> timescales = new LinkedList<>();
    final Timescale ts1 = new Timescale(2, 2);
    final Timescale ts2 = new Timescale(4, 4);
    final Timescale ts3 = new Timescale(8, 8);
    timescales.add(ts1);
    timescales.add(ts2);
    timescales.add(ts3);
    final long startTime = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime());
    final Map<Timescale, Queue<MTSWindowOutput<Map<Integer, Long>>>> results = new HashMap<>();
    results.put(ts1, new LinkedList<MTSWindowOutput<Map<Integer, Long>>>());
    results.put(ts2, new LinkedList<MTSWindowOutput<Map<Integer, Long>>>());
    results.put(ts3, new LinkedList<MTSWindowOutput<Map<Integer, Long>>>());

    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(InitialStartTime.class, Long.toString(startTime));
    jcb.bindImplementation(KeyExtractor.class, IntegerExtractor.class);
    jcb.bindImplementation(ComAndAscAggregator.class, CountByKeyAggregator.class);
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    injector.bindVolatileInstance(List.class, timescales);
    injector.bindVolatileInstance(MTSWindowOperator.MTSOutputHandler.class,
        new TestOutputHandler(monitor, results, startTime));
    final MTSWindowOperator<Integer> operator = injector.getInstance(StaticMTSOperatorImpl.class);

    operator.start();
    executor.submit(new Runnable() {
      @Override
      public void run() {
        while (!finished.booleanValue()) {
          operator.execute(Math.abs(random.nextInt() % 10));
          try {
            Thread.sleep(10);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }
    });

    monitor.mwait();
    operator.close();

    // check outputs
    while (!results.get(ts3).isEmpty()) {
      final MTSWindowOutput<Map<Integer, Long>> ts3Output = results.get(ts3).poll();
      final MTSWindowOutput<Map<Integer, Long>> ts2Output1 = results.get(ts2).poll();
      final MTSWindowOutput<Map<Integer, Long>> ts2Output2 = results.get(ts2).poll();
      Assert.assertEquals(MTSTestUtils.merge(ts2Output1.output, ts2Output2.output), ts3Output.output);

      final MTSWindowOutput<Map<Integer, Long>> ts1Output1 = results.get(ts1).poll();
      final MTSWindowOutput<Map<Integer, Long>> ts1Output2 = results.get(ts1).poll();
      Assert.assertEquals(MTSTestUtils.merge(ts1Output1.output, ts1Output2.output), ts2Output1.output);

      final MTSWindowOutput<Map<Integer, Long>> ts1Output3 = results.get(ts1).poll();
      final MTSWindowOutput<Map<Integer, Long>> ts1Output4 = results.get(ts1).poll();
      Assert.assertEquals(MTSTestUtils.merge(ts1Output3.output, ts1Output4.output), ts2Output2.output);
    }
  }
}
