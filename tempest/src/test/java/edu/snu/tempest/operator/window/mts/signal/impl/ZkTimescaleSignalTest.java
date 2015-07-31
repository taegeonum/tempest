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
package edu.snu.tempest.operator.window.mts.signal.impl;

import edu.snu.tempest.operator.window.Timescale;
import edu.snu.tempest.operator.window.mts.TimescaleSignalListener;
import edu.snu.tempest.operator.window.mts.signal.MTSSignalReceiver;
import edu.snu.tempest.operator.window.mts.signal.MTSSignalSender;
import edu.snu.tempest.util.Monitor;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class ZkTimescaleSignalTest {

  private final String identifier = "test-signal";
  private final int port = 2181;
  private final String host = "localhost";

  /**
   * ZkSignalSender sends three timescales to Zookeeper.
   * ZKSignalReceiver receives the timescales. 
   */
  @Test
  public void addTimescaleSignalTest() throws Exception {
    final Monitor monitor = new Monitor();
    final ConcurrentLinkedQueue<Timescale> timescales = new ConcurrentLinkedQueue<>();
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.bindNamedParameter(ZkMTSParameters.OperatorIdentifier.class, identifier);
    cb.bindNamedParameter(ZkMTSParameters.ZkServerAddress.class, host + ":" + port);
    cb.bindImplementation(MTSSignalReceiver.class, ZkSignalReceiver.class);
    
    final Injector injector = Tang.Factory.getTang().newInjector(cb.build());
    final MTSSignalReceiver connector = injector.getInstance(MTSSignalReceiver.class);
    connector.addTimescaleSignalListener(new TestTimescaleListener(timescales, monitor, 3));
    connector.start();
    
    cb.bindImplementation(MTSSignalSender.class, ZkSignalSender.class);
    final Injector injector2 = Tang.Factory.getTang().newInjector(cb.build());
    final MTSSignalSender client = injector2.getInstance(MTSSignalSender.class);

    final Timescale t1 = new Timescale(5, 1);
    final Timescale t2 = new Timescale(10, 2);
    final Timescale t3 = new Timescale(15, 3);

    client.addTimescale(t1);
    Thread.sleep(500);
    client.addTimescale(t2);
    Thread.sleep(500);
    client.addTimescale(t3);
    monitor.mwait();
    
    Assert.assertEquals(t1, timescales.poll());
    Assert.assertEquals(t2, timescales.poll());
    Assert.assertEquals(t3, timescales.poll());

    client.close();
    connector.close();
  }
  
  /**
   * Add two timescales and remove one timescale. 
   */
  @Test
  public void removeTimescaleSignalTest() throws Exception {
    final Monitor monitor = new Monitor();
    final ConcurrentLinkedQueue<Timescale> timescales = new ConcurrentLinkedQueue<>();
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.bindNamedParameter(ZkMTSParameters.OperatorIdentifier.class, identifier);
    cb.bindNamedParameter(ZkMTSParameters.ZkServerAddress.class, host + ":" + port);
    cb.bindImplementation(MTSSignalReceiver.class, ZkSignalReceiver.class);
    
    final Injector injector = Tang.Factory.getTang().newInjector(cb.build());
    final MTSSignalReceiver connector = injector.getInstance(MTSSignalReceiver.class);
    connector.addTimescaleSignalListener(new TestTimescaleListener(timescales, monitor, 3));
    connector.start();
    cb.bindImplementation(MTSSignalSender.class, ZkSignalSender.class);
    final Injector injector2 = Tang.Factory.getTang().newInjector(cb.build());
    final MTSSignalSender client = injector2.getInstance(MTSSignalSender.class);

    final Timescale t1 = new Timescale(5, 1);
    final Timescale t2 = new Timescale(10, 2);

    client.addTimescale(t1);
    Thread.sleep(500);
    client.addTimescale(t2);
    Thread.sleep(500);
    client.removeTimescale(t2);
    monitor.mwait();

    Assert.assertEquals(timescales.size(), 1);
    Assert.assertEquals(t1, timescales.poll());
    client.close();
    connector.close();

  }
  
  static class TestTimescaleListener implements TimescaleSignalListener {
    private final Collection<Timescale> list;
    private final Monitor monitor;
    private final AtomicInteger counter;
    
    public TestTimescaleListener(final Collection<Timescale> list,  final Monitor monitor, final int numOfTimescales) {
      this.list = list;
      this.monitor = monitor;
      this.counter = new AtomicInteger(numOfTimescales);
    }

    @Override
    public void onTimescaleAddition(final Timescale ts, final long startTime) {
      this.list.add(ts);
      if (this.counter.decrementAndGet() == 0) {
        monitor.mnotify();
      }
    }

    @Override
    public void onTimescaleDeletion(final Timescale ts) {
      this.list.remove(ts);
      if (this.counter.decrementAndGet() == 0) {
        monitor.mnotify();
      }
    }
  }
}
