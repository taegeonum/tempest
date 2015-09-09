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
package edu.snu.tempest.example;

import edu.snu.tempest.operator.window.aggregator.impl.CountByKeyAggregator;
import edu.snu.tempest.operator.window.aggregator.impl.KeyExtractor;
import edu.snu.tempest.operator.window.timescale.DynamicMTSWindowConfiguration;
import edu.snu.tempest.operator.window.timescale.DynamicMTSWindowOperator;
import edu.snu.tempest.operator.window.timescale.Timescale;
import edu.snu.tempest.operator.window.timescale.impl.TimescaleParser;
import edu.snu.tempest.operator.LoggingOutputEmitter;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.wake.impl.StageManager;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * This example creates DynamicMTSOperatorImpl
 * and sends integer values to the operator.
 * The mts operator calculates the integer by key.
 * Also, this example adds 3 timescales dynamically to the operator.
 */
public final class MTSWindowOperatorExample {

  private MTSWindowOperatorExample() {
  }
  
  public static void main(String[] args) throws Exception {
    final ExecutorService executor = Executors.newFixedThreadPool(1);
    final Timescale ts = new Timescale(5, 3);
    final List<Timescale> list = new LinkedList<>();
    list.add(ts);
    final long startTime = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime());

    final Configuration operatorConf = DynamicMTSWindowConfiguration.CONF
        .set(DynamicMTSWindowConfiguration.START_TIME, startTime)
        .set(DynamicMTSWindowConfiguration.INITIAL_TIMESCALES, TimescaleParser.parseToString(list))
        .set(DynamicMTSWindowConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
        .build();

    final Injector ij = Tang.Factory.getTang().newInjector(operatorConf);
    ij.bindVolatileInstance(KeyExtractor.class, new KeyExtractor<Integer, Integer>() {
      @Override
      public Integer getKey(final Integer input) {
        return input;
      }
    });
    final DynamicMTSWindowOperator<Integer, Map<Integer, Integer>> operator =
        ij.getInstance(DynamicMTSWindowOperator.class);
    operator.prepare(new LoggingOutputEmitter());
    // add input
    executor.submit(new Runnable() {
      @Override
      public void run() {
        final Random rand = new Random();
        for (int i = 0; i < 10000; i++) {
          operator.execute(Math.abs(rand.nextInt() % 5));
          try {
            Thread.sleep(10);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }
    });

    // add timescales to the MTS operator
    Thread.sleep(6000);
    operator.onTimescaleAddition(new Timescale(10, 5), TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()));
    Thread.sleep(10000);
    operator.onTimescaleAddition(new Timescale(20, 8), TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()));
    Thread.sleep(10000);
    operator.onTimescaleAddition(new Timescale(15, 7), TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()));
    executor.shutdown();
    executor.awaitTermination(30, TimeUnit.SECONDS);
    StageManager.instance().close();
  }
}
