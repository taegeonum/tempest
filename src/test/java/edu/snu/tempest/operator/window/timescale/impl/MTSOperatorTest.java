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
package edu.snu.tempest.operator.window.timescale.impl;


import edu.snu.tempest.operator.OperatorConnector;
import edu.snu.tempest.operator.Operator;
import edu.snu.tempest.operator.window.timescale.TimeWindowOutputHandler;
import edu.snu.tempest.operator.window.aggregator.impl.CountByKeyAggregator;
import edu.snu.tempest.operator.window.aggregator.impl.KeyExtractor;
import edu.snu.tempest.operator.window.timescale.DynamicMTSWindowConfiguration;
import edu.snu.tempest.operator.window.timescale.StaticMTSWindowConfiguration;
import edu.snu.tempest.operator.window.timescale.Timescale;
import edu.snu.tempest.operator.window.timescale.TimescaleWindowOutput;
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
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindImplementation(Operator.class, DynamicMTSOperatorImpl.class);
    multipleTimescaleAggregationTest(Configurations.merge(jcb.build(), DynamicMTSWindowConfiguration.CONF
        .set(DynamicMTSWindowConfiguration.START_TIME, startTime)
        .set(DynamicMTSWindowConfiguration.INITIAL_TIMESCALES, TimescaleParser.parseToString(timescales))
        .set(DynamicMTSWindowConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
        .build()));
  }

  @Test
  public void staticMTSOperationTest() throws Exception {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindImplementation(Operator.class, StaticMTSOperatorImpl.class);
    multipleTimescaleAggregationTest(Configurations.merge(jcb.build(), StaticMTSWindowConfiguration.CONF
        .set(StaticMTSWindowConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
        .set(StaticMTSWindowConfiguration.INITIAL_TIMESCALES, TimescaleParser.parseToString(timescales))
        .set(StaticMTSWindowConfiguration.START_TIME, startTime)
        .build()));
  }

  /**
   * Aggregates multi-time scale outputs.
   */
  private void multipleTimescaleAggregationTest(final Configuration operatorConf) throws Exception {
    final ExecutorService executor = Executors.newFixedThreadPool(1);
    final Monitor monitor = new Monitor();
    final Boolean finished = false;
    final Random random = new Random();

    final Map<Timescale, Queue<TimescaleWindowOutput<Map<Integer, Long>>>> results = new HashMap<>();
    results.put(ts1, new LinkedList<TimescaleWindowOutput<Map<Integer, Long>>>());
    results.put(ts2, new LinkedList<TimescaleWindowOutput<Map<Integer, Long>>>());
    results.put(ts3, new LinkedList<TimescaleWindowOutput<Map<Integer, Long>>>());

    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindImplementation(KeyExtractor.class, IntegerExtractor.class);

    final Injector injector = Tang.Factory.getTang().newInjector(
        Configurations.merge(jcb.build(), operatorConf));
    injector.bindVolatileInstance(Monitor.class, monitor);
    injector.bindVolatileInstance(Map.class, results);

    final Operator<Integer, TimescaleWindowOutput<Map<Integer, Long>>>
        operator = injector.getInstance(Operator.class);
    final TimeWindowOutputHandler<Map<Integer, Long>, Map<Integer, Long>>
        tsOutputHandler = injector.getInstance(TestOutputHandler.class);
    operator.prepare(new OperatorConnector<>(tsOutputHandler));
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
      final TimescaleWindowOutput<Map<Integer, Long>> ts3Output = results.get(ts3).poll();
      final TimescaleWindowOutput<Map<Integer, Long>> ts2Output1 = results.get(ts2).poll();
      final TimescaleWindowOutput<Map<Integer, Long>> ts2Output2 = results.get(ts2).poll();
      Assert.assertEquals(MTSTestUtils.merge(ts2Output1.output, ts2Output2.output), ts3Output.output);

      final TimescaleWindowOutput<Map<Integer, Long>> ts1Output1 = results.get(ts1).poll();
      final TimescaleWindowOutput<Map<Integer, Long>> ts1Output2 = results.get(ts1).poll();
      Assert.assertEquals(MTSTestUtils.merge(ts1Output1.output, ts1Output2.output), ts2Output1.output);

      final TimescaleWindowOutput<Map<Integer, Long>> ts1Output3 = results.get(ts1).poll();
      final TimescaleWindowOutput<Map<Integer, Long>> ts1Output4 = results.get(ts1).poll();
      Assert.assertEquals(MTSTestUtils.merge(ts1Output3.output, ts1Output4.output), ts2Output2.output);
    }
  }
}
