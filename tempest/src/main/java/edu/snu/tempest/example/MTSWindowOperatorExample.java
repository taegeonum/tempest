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

import edu.snu.tempest.operator.window.WindowOperator;
import edu.snu.tempest.operator.window.aggregator.impl.CountByKeyAggregator;
import edu.snu.tempest.operator.window.aggregator.impl.KeyExtractor;
import edu.snu.tempest.operator.window.time.*;
import edu.snu.tempest.signal.window.time.TimescaleSignal;
import edu.snu.tempest.signal.TempestSignalSenderStage;
import edu.snu.tempest.signal.impl.ZkSignalSenderStage;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.wake.impl.StageManager;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;
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

    // configure zookeeper setting.
    final String identifier = "mts-test";
    final String address = "localhost:2181";

    final Configuration operatorConf = DynamicMTSWindowConfiguration.CONF
        .set(DynamicMTSWindowConfiguration.START_TIME, startTime)
        .set(DynamicMTSWindowConfiguration.INITIAL_TIMESCALES, TimescaleParser.parseToString(list))
        .set(DynamicMTSWindowConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
        .set(DynamicMTSWindowConfiguration.OUTPUT_HANDLER, TestHandler.class)
        .set(DynamicMTSWindowConfiguration.OPERATOR_IDENTIFIER, identifier)
        .set(DynamicMTSWindowConfiguration.ZK_SERVER_ADDRESS, address)
        .build();

    final Injector ij = Tang.Factory.getTang().newInjector(operatorConf);
    ij.bindVolatileInstance(KeyExtractor.class, new KeyExtractor<Integer, Integer>() {
      @Override
      public Integer getKey(final Integer input) {
        return input;
      }
    });
    final WindowOperator<Integer> operator = ij.getInstance(WindowOperator.class);
    final TempestSignalSenderStage<TimescaleSignal> sender = ij.getInstance(ZkSignalSenderStage.class);

    // add input
    executor.submit(new Runnable() {
      @Override
      public void run() {
        final Random rand = new Random();
        for (int i = 0; i < 25000; i++) {
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
    Thread.sleep(3000);
    sender.sendSignal(identifier, new TimescaleSignal(10, 5,
        TimescaleSignal.ADDITION, TimeUnit.NANOSECONDS.toSeconds(System.nanoTime())));
    Thread.sleep(4000);
    sender.sendSignal(identifier, new TimescaleSignal(20, 8,
        TimescaleSignal.ADDITION, TimeUnit.NANOSECONDS.toSeconds(System.nanoTime())));
    Thread.sleep(5000);
    sender.sendSignal(identifier, new TimescaleSignal(15, 7,
        TimescaleSignal.ADDITION, TimeUnit.NANOSECONDS.toSeconds(System.nanoTime())));
    executor.shutdown();
    executor.awaitTermination(30, TimeUnit.SECONDS);
    sender.close();
    StageManager.instance().close();
  }

  public static final class TestHandler implements TimeWindowOutputHandler<Integer> {

    @Inject
    private TestHandler() {
    }

    @Override
    public void onNext(final TimeWindowOutput<Integer> output) {
      System.out.println(output);
    }
  }
}
