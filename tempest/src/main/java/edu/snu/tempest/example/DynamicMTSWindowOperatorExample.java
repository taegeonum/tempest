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
package edu.snu.tempest.example;

import edu.snu.tempest.operator.window.aggregator.AssociativeAggregator;
import edu.snu.tempest.operator.window.aggregator.impl.CountByKeyAggregator;
import edu.snu.tempest.operator.window.aggregator.impl.KeyExtractor;
import edu.snu.tempest.operator.window.time.Timescale;
import edu.snu.tempest.operator.window.time.mts.MTSWindowOperator;
import edu.snu.tempest.operator.window.time.mts.MTSWindowOutput;
import edu.snu.tempest.operator.window.time.mts.impl.DynamicMTSOperatorImpl;
import edu.snu.tempest.operator.window.time.mts.parameters.StartTime;
import edu.snu.tempest.operator.window.time.mts.signal.MTSSignalSender;
import edu.snu.tempest.operator.window.time.mts.signal.impl.ZkMTSParameters;
import edu.snu.tempest.operator.window.time.mts.signal.impl.ZkSignalSender;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;

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
 * Also, this example add adds 3 timescales dynamically to the operator.
 */
public final class DynamicMTSWindowOperatorExample {

  private DynamicMTSWindowOperatorExample() {
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

    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.bindNamedParameter(ZkMTSParameters.OperatorIdentifier.class, identifier);
    cb.bindNamedParameter(ZkMTSParameters.ZkServerAddress.class, address);
    cb.bindImplementation(AssociativeAggregator.class, CountByKeyAggregator.class);
    cb.bindNamedParameter(StartTime.class, Long.toString(startTime));

    final Injector ij = Tang.Factory.getTang().newInjector(cb.build());
    ij.bindVolatileInstance(KeyExtractor.class, new KeyExtractor<Integer, Integer>() {
      @Override
      public Integer getKey(final Integer input) {
        return input;
      }
    });
    ij.bindVolatileInstance(List.class, list);
    ij.bindVolatileInstance(MTSWindowOperator.MTSOutputHandler.class, new TestHandler());
    ij.bindVolatileInstance(Long.class, startTime);
    final MTSWindowOperator<Integer> operator = ij.getInstance(DynamicMTSOperatorImpl.class);
    final MTSSignalSender sender = ij.getInstance(ZkSignalSender.class);

    // start mts operator
    operator.start();
    // add input
    executor.submit(new Runnable() {
      @Override
      public void run() {
        final Random rand = new Random();
        for (int i = 0; i < 3000; i++) {
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
    sender.addTimescale(new Timescale(10, 5));
    Thread.sleep(4000);
    sender.addTimescale(new Timescale(20, 8));
    Thread.sleep(5000);
    sender.addTimescale(new Timescale(15, 7));
    
    executor.shutdown();
    executor.awaitTermination(30, TimeUnit.SECONDS);
    operator.close();
    sender.close();
  }

  private static final class TestHandler implements MTSWindowOperator.MTSOutputHandler<Integer> {

    private TestHandler() {
    }

    @Override
    public void onNext(final MTSWindowOutput<Integer> output) {
      System.out.println(output);
    }
  }
}
