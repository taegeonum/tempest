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
import edu.snu.tempest.operators.dynamicmts.TimescaleSignalListener;
import edu.snu.tempest.operators.dynamicmts.impl.DynamicMTSOperatorImpl;
import edu.snu.tempest.operators.staticmts.MTSOperator;
import edu.snu.tempest.operators.dynamicmts.signal.MTSSignalReceiver;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Dynamic MTSOperator Example.
 */
public final class MTSOperatorExample {

  private MTSOperatorExample() {

  }

  public static void main(String[] args) throws Exception {
    final Timescale ts = new Timescale(5, 3);
    final Aggregator<Integer, Integer> testAggregator = new TestAggregator();
    final List<Timescale> list = new LinkedList<>();
    list.add(ts);
    final long startTime = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime());
    final DynamicMTSOperator<Integer> operator =
        new DynamicMTSOperatorImpl<>(testAggregator, list,
           new TestHandler(), new Receiver(), 0, startTime);

    operator.start();
    final Random rand = new Random();
    for (int i = 0; i < 1500; i++) {
      operator.execute(Math.abs(rand.nextInt() % 5));
      
      if (i == 200) {
        operator.onTimescaleAddition(new Timescale(10, 2), startTime);
      }
      
      if (i == 500) {
        operator.onTimescaleAddition(new Timescale(7, 4), startTime);
      }
      Thread.sleep(10);
    }
    operator.close();
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
      for (final Integer partial : partials) {
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

  private static final class Receiver implements MTSSignalReceiver {
    private Receiver() {

    }

    @Override
    public void start() throws Exception {

    }

    @Override
    public void addTimescaleSignalListener(final TimescaleSignalListener listener) {

    }

    @Override
    public void close() throws Exception {

    }
  }
}
