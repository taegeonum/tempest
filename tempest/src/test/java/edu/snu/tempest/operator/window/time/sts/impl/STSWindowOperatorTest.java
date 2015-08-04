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
package edu.snu.tempest.operator.window.time.sts.impl;

import edu.snu.tempest.operator.window.aggregator.CAAggregator;
import edu.snu.tempest.operator.window.aggregator.impl.CountByKeyAggregator;
import edu.snu.tempest.operator.window.aggregator.impl.KeyExtractor;
import edu.snu.tempest.operator.window.time.Timescale;
import edu.snu.tempest.operator.window.time.mts.parameters.StartTime;
import edu.snu.tempest.operator.window.time.sts.STSWindowOperator;
import edu.snu.tempest.operator.window.time.sts.STSWindowOutput;
import edu.snu.tempest.util.test.IntegerExtractor;
import edu.snu.tempest.util.test.Monitor;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class STSWindowOperatorTest {
  private static final Logger LOG = Logger.getLogger(STSWindowOperatorTest.class.getName());

  /**
   * Aggregates single timescale outputs.
   */
  @Test
  public void aggregationTest() throws Exception {
    final ExecutorService executor = Executors.newFixedThreadPool(1);
    final Monitor monitor = new Monitor();
    final Boolean finished = new Boolean(false);
    final Random random = new Random();
    final Timescale ts1 = new Timescale(4, 2);
    final long startTime = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime());
    final Map<Timescale, Queue<STSWindowOutput<Map<Integer, Long>>>> results = new HashMap<>();
    results.put(ts1, new LinkedList<STSWindowOutput<Map<Integer, Long>>>());

    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(StartTime.class, Long.toString(startTime));
    jcb.bindImplementation(KeyExtractor.class, IntegerExtractor.class);
    jcb.bindImplementation(CAAggregator.class, CountByKeyAggregator.class);
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    injector.bindVolatileInstance(Timescale.class, ts1);
    injector.bindVolatileInstance(STSWindowOperator.STSOutputHandler.class,
        new TestOutputHandler(monitor));
    final STSWindowOperator<Integer> operator = injector.getInstance(STSWindowOperatorImpl.class);

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
  }

  public final class TestOutputHandler implements STSWindowOperator.STSOutputHandler<Map<Integer, Long>> {
    private final Monitor monitor;
    private int count = 0;

    public TestOutputHandler(final Monitor monitor) {
      this.monitor = monitor;
    }

    @Override
    public void onNext(final STSWindowOutput<Map<Integer, Long>> windowOutput) {
      if (count == 3) {
        this.monitor.mnotify();
      }
      LOG.log(Level.INFO, windowOutput.output.toString());
      count++;
    }
  }
}