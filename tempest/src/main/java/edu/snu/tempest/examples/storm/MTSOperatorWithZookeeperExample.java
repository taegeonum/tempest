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
package edu.snu.tempest.examples.storm;

import edu.snu.tempest.operators.Timescale;
import edu.snu.tempest.operators.common.Aggregator;
import edu.snu.tempest.operators.common.MTSWindowOutput;
import edu.snu.tempest.operators.dynamicmts.DynamicMTSOperator;
import edu.snu.tempest.operators.dynamicmts.impl.DynamicMTSOperatorImpl;
import edu.snu.tempest.operators.dynamicmts.signal.impl.ZkMTSParameters;
import edu.snu.tempest.operators.dynamicmts.signal.impl.ZkSignalSender;
import edu.snu.tempest.operators.staticmts.MTSOperator;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import edu.snu.tempest.operators.dynamicmts.signal.MTSSignalSender;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Creates DynamicMTSOperatorImpl and adds timescales by using ZkSignalSender.
 * 
 */
public final class MTSOperatorWithZookeeperExample {

  private MTSOperatorWithZookeeperExample() {

  }
  
  public static void main(String[] args) throws Exception {
    final ExecutorService executor = Executors.newFixedThreadPool(1);
    final Timescale ts = new Timescale(5, 3);
    final List<Timescale> list = new LinkedList<>();
    list.add(ts);
    final long startTime = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime());
    final Aggregator<Integer, Integer> testAggregator = new TestAggregator();

    // configure zookeeper setting.
    final String identifier = "mts-test";
    final String address = "localhost:2181";

    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.bindNamedParameter(ZkMTSParameters.OperatorIdentifier.class, identifier);
    cb.bindNamedParameter(ZkMTSParameters.ZkServerAddress.class, address);

    final Injector ij = Tang.Factory.getTang().newInjector(cb.build());
    ij.bindVolatileInstance(Aggregator.class, testAggregator);
    ij.bindVolatileInstance(List.class, list);
    ij.bindVolatileInstance(MTSOperator.MTSOutputHandler.class, new TestHandler());
    ij.bindVolatileInstance(Long.class, startTime);
    final DynamicMTSOperator<Integer> operator = ij.getInstance(DynamicMTSOperatorImpl.class);
    final MTSSignalSender sender = ij.getInstance(ZkSignalSender.class);

    operator.start();
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
    Thread.sleep(1500);
    sender.addTimescale(new Timescale(10, 5));
    Thread.sleep(1000);
    sender.addTimescale(new Timescale(20, 8));
    Thread.sleep(1000);
    sender.addTimescale(new Timescale(15, 7));
    
    executor.shutdown();
    executor.awaitTermination(30, TimeUnit.SECONDS);
    operator.close();
    sender.close();
  }


  private static final class TestAggregator implements Aggregator<Integer, Integer> {

    private TestAggregator() {

    }

    @Override
    public Integer init() {
      return 0;
    }

    @Override
    public Integer partialAggregate(final Integer oldVal, final Integer newVal) {
      return oldVal + newVal;
    }

    @Override
    public Integer finalAggregate(final List<Integer> partials) {
      int sum = 0;
      for (Integer partial : partials) {
        sum += partial;
      }
      return sum;
    }
  }

  private static final class TestHandler implements MTSOperator.MTSOutputHandler<Integer> {

    private TestHandler() {

    }

    @Override
    public void onNext(final MTSWindowOutput<Integer> output) {
      System.out.println(output);
    }
  }
}
