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
package edu.snu.tempest.operator.window.time.signal.impl;

import edu.snu.tempest.operator.window.time.Timescale;
import edu.snu.tempest.operator.window.time.signal.MTSSignalSender;
import edu.snu.tempest.operator.window.time.signal.TimescaleSignal;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.remote.Encoder;
import org.apache.zookeeper.data.Stat;

import javax.inject.Inject;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * MTSSignalSender implementation using Zookeeper.
 */
public final class ZkSignalSender implements MTSSignalSender {

  private static final Logger LOG = Logger.getLogger(ZkSignalSender.class.getName());

  /**
   * A curator framework for sending messages to zookeeper.
   */
  private final CuratorFramework client;

  /**
   * An encoder for timescale signal.
   */
  private final Encoder<TimescaleSignal> encoder;

  /**
   * An identifier of mts operator.
   */
  private final String identifier;

  /**
   * MTSSignalSender implementation using Zookeeper.
   * @param address a zookeeper address
   * @param identifier an identifier of mts operator
   * @param encoder an encoder for timescale signal
   */
  @Inject
  private ZkSignalSender(
      @Parameter(ZkMTSParameters.ZkServerAddress.class) final String address,
      @Parameter(ZkMTSParameters.OperatorIdentifier.class) final String identifier,
      @Parameter(ZkMTSParameters.ZkTSEncoder.class) final Encoder<TimescaleSignal> encoder) {
    LOG.log(Level.INFO, "Creates ZookeeperMTSClient");
    this.client = CuratorFrameworkFactory.builder().namespace(ZkSignalReceiver.NAMESPACE).connectString(address)
        .retryPolicy(new RetryOneTime(500)).build();
    this.encoder = encoder;
    this.identifier = identifier;
    this.client.start();
  }
  
  @Override
  public void close() throws Exception {
    this.client.close();
  }

  /**
   * Send timescale information to zookeeper by usign curator framework.
   * @param ts a timescale
   * @param type type (addition/deletion)
   * @throws Exception
   */
  private void sendTimescaleInfo(final TimescaleSignal ts, final String type) throws Exception {
    Stat stat = this.client.checkExists().forPath(this.identifier + "-" + type);
    if (stat == null) {
      String path = this.client.create().creatingParentsIfNeeded().forPath(this.identifier + "-" + type);
      LOG.log(Level.INFO, "Zookeeper path Created: " + path);
    }
    this.client.setData().forPath(this.identifier + "-" + type, encoder.encode(ts));
  }

  /**
   * Add a timescale by using zookeeper.
   * @param ts timescale to be added.
   * @throws Exception
   */
  @Override
  public void addTimescale(final Timescale ts) throws Exception {
    sendTimescaleInfo(new TimescaleSignal(ts,
        TimeUnit.NANOSECONDS.toSeconds(System.nanoTime())), "addition");
    LOG.log(Level.INFO, "Add timescale: " + ts);
  }

  /**
   * Remove a timescale  by using zookeeper.
   * @param ts timescale to be deleted.
   * @throws Exception
   */
  @Override
  public void removeTimescale(final Timescale ts) throws Exception {
    sendTimescaleInfo(new TimescaleSignal(ts,
        TimeUnit.NANOSECONDS.toSeconds(System.nanoTime())), "deletion");
    LOG.log(Level.INFO, "Remove timescale: " + ts);
  }
}
