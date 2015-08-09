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
package edu.snu.tempest.signal.impl;

import edu.snu.tempest.signal.TempestSignalReceiverStage;
import edu.snu.tempest.signal.TempestSignalSenderStage;
import edu.snu.tempest.test.util.Monitor;
import edu.snu.tempest.test.util.TestSignal;
import edu.snu.tempest.test.util.TestSignalCodec;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.wake.EventHandler;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class ZkSignalTest {

  private final String identifier = "test-signal";
  private final int port = 2181;
  private final String host = "localhost";

  /**
   * ZkSignalSender sends three signals.
   */
  @Test
  public void sendSignalTest() throws Exception {
    final Monitor monitor = new Monitor();
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.bindNamedParameter(ZkMTSParameters.ZkServerAddress.class, host + ":" + port);
    cb.bindImplementation(TempestSignalReceiverStage.class, ZkSignalReceiverStage.class);
    cb.bindImplementation(TempestSignalSenderStage.class, ZkSignalSenderStage.class);
    cb.bindNamedParameter(ZkMTSParameters.ZkSignalDecoder.class, TestSignalCodec.class);
    cb.bindNamedParameter(ZkMTSParameters.ZkSignalEncoder.class, TestSignalCodec.class);

    final Injector injector = Tang.Factory.getTang().newInjector(cb.build());
    final TempestSignalSenderStage<TestSignal> sender = injector.getInstance(TempestSignalSenderStage.class);
    final TempestSignalReceiverStage<TestSignal> receiver = injector.getInstance(TempestSignalReceiverStage.class);
    receiver.registerHandler(identifier, new TestTimescaleListener(monitor, 3));


    sender.sendSignal(identifier, new TestSignal());
    Thread.sleep(500);
    sender.sendSignal(identifier, new TestSignal());
    Thread.sleep(500);
    sender.sendSignal(identifier, new TestSignal());
    monitor.mwait();

    sender.close();
    receiver.close();
  }
  
  static class TestTimescaleListener implements EventHandler<TestSignal> {
    private final Monitor monitor;
    private final AtomicInteger counter;
    
    public TestTimescaleListener(final Monitor monitor, final int numOfTimescales) {
      this.monitor = monitor;
      this.counter = new AtomicInteger(numOfTimescales);
    }

    @Override
    public void onNext(final TestSignal ts) {
      if (this.counter.decrementAndGet() == 0) {
        monitor.mnotify();
      }
    }
  }
}
