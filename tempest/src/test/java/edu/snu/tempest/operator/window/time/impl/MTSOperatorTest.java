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
package edu.snu.tempest.operator.window.time.impl;


import edu.snu.tempest.operator.window.WindowOperator;
import edu.snu.tempest.operator.window.aggregator.impl.CountByKeyAggregator;
import edu.snu.tempest.operator.window.aggregator.impl.KeyExtractor;
import edu.snu.tempest.operator.window.time.*;
import edu.snu.tempest.test.util.IntegerExtractor;
import edu.snu.tempest.test.util.MTSTestUtils;
import edu.snu.tempest.test.util.Monitor;
import edu.snu.tempest.test.util.TestOutputHandler;
import org.apache.reef.tang.*;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class MTSOperatorTest {

  List<Timescale> timescales;
  Timescale ts1;
  Timescale ts2;
  Timescale ts3;
  long startTime;

  @Before
  public void initialize() throws InjectionException {
    startTime = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime());
    timescales = new LinkedList<>();
    // timescales
    ts1 = new Timescale(2, 2);
    ts2 = new Timescale(4, 4);
    ts3 = new Timescale(8, 8);
    timescales.add(ts1);
    timescales.add(ts2);
    timescales.add(ts3);
  }

  @Test
  public void dynamicMTSOperationTest() throws Exception {
    multipleTimescaleAggregationTest(DynamicMTSWindowConfiguration.CONF
        .set(DynamicMTSWindowConfiguration.START_TIME, startTime)
        .set(DynamicMTSWindowConfiguration.INITIAL_TIMESCALES, TimescaleParser.parseToString(timescales))
        .set(DynamicMTSWindowConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
        .set(DynamicMTSWindowConfiguration.OUTPUT_HANDLER, TestOutputHandler.class)
        .set(DynamicMTSWindowConfiguration.OPERATOR_IDENTIFIER, "test")
        .set(DynamicMTSWindowConfiguration.ZK_SERVER_ADDRESS, "localhost:2181")
        .build());
  }

  @Test
  public void staticMTSOperationTest() throws Exception {
    multipleTimescaleAggregationTest(StaticMTSWindowConfiguration.CONF
        .set(StaticMTSWindowConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
        .set(StaticMTSWindowConfiguration.INITIAL_TIMESCALES, TimescaleParser.parseToString(timescales))
        .set(StaticMTSWindowConfiguration.OUTPUT_HANDLER, TestOutputHandler.class)
        .set(StaticMTSWindowConfiguration.START_TIME, startTime)
        .build());
  }

  /**
   * Aggregates multi-time scale outputs.
   */
  private void multipleTimescaleAggregationTest(final Configuration operatorConf) throws Exception {
    final ExecutorService executor = Executors.newFixedThreadPool(1);
    final Monitor monitor = new Monitor();
    final Boolean finished = false;
    final Random random = new Random();

    final Map<Timescale, Queue<TimeWindowOutput<Map<Integer, Long>>>> results = new HashMap<>();
    results.put(ts1, new LinkedList<TimeWindowOutput<Map<Integer, Long>>>());
    results.put(ts2, new LinkedList<TimeWindowOutput<Map<Integer, Long>>>());
    results.put(ts3, new LinkedList<TimeWindowOutput<Map<Integer, Long>>>());

    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindImplementation(KeyExtractor.class, IntegerExtractor.class);

    final Injector injector = Tang.Factory.getTang().newInjector(
        Configurations.merge(jcb.build(), operatorConf));
    injector.bindVolatileInstance(Monitor.class, monitor);
    injector.bindVolatileInstance(Map.class, results);

    final WindowOperator<Integer> operator = injector.getInstance(WindowOperator.class);
    executor.submit(new Runnable() {
      @Override
      public void run() {
        while (!finished.booleanValue()) {
          operator.execute(Math.abs(random.nextInt() % 10));
          try {
            Thread.sleep(10);
          } catch (final InterruptedException e) {
            e.printStackTrace();
          }
        }
      }
    });

    monitor.mwait();

    // check outputs
    while (!results.get(ts3).isEmpty()) {
      final TimeWindowOutput<Map<Integer, Long>> ts3Output = results.get(ts3).poll();
      final TimeWindowOutput<Map<Integer, Long>> ts2Output1 = results.get(ts2).poll();
      final TimeWindowOutput<Map<Integer, Long>> ts2Output2 = results.get(ts2).poll();
      Assert.assertEquals(MTSTestUtils.merge(ts2Output1.output, ts2Output2.output), ts3Output.output);

      final TimeWindowOutput<Map<Integer, Long>> ts1Output1 = results.get(ts1).poll();
      final TimeWindowOutput<Map<Integer, Long>> ts1Output2 = results.get(ts1).poll();
      Assert.assertEquals(MTSTestUtils.merge(ts1Output1.output, ts1Output2.output), ts2Output1.output);

      final TimeWindowOutput<Map<Integer, Long>> ts1Output3 = results.get(ts1).poll();
      final TimeWindowOutput<Map<Integer, Long>> ts1Output4 = results.get(ts1).poll();
      Assert.assertEquals(MTSTestUtils.merge(ts1Output3.output, ts1Output4.output), ts2Output2.output);
    }
  }
}
