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

import edu.snu.tempest.signal.TempestSignalSenderStage;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.remote.Encoder;
import org.apache.zookeeper.data.Stat;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * SignalSender implementation using Zookeeper.
 */
public final class ZkSignalSenderStage<T> implements TempestSignalSenderStage<T> {

  private static final Logger LOG = Logger.getLogger(ZkSignalSenderStage.class.getName());

  /**
   * A curator framework for sending messages to zookeeper.
   */
  private final CuratorFramework client;

  /**
   * An encoder for timescale signal.
   */
  private final Encoder<T> encoder;

  /**
   * MTSSignalSender implementation using Zookeeper.
   * @param address a zookeeper address
   * @param encoder an encoder for timescale signal
   */
  @Inject
  private ZkSignalSenderStage(
      @Parameter(ZkMTSParameters.ZkServerAddress.class) final String address,
      @Parameter(ZkMTSParameters.ZkSignalEncoder.class) final Encoder<T> encoder) {
    LOG.log(Level.INFO, "Creates ZookeeperMTSClient");
    this.client = CuratorFrameworkFactory.builder().namespace(ZkSignalReceiverStage.NAMESPACE).connectString(address)
        .retryPolicy(new RetryOneTime(500)).build();
    this.encoder = encoder;
    this.client.start();
  }
  
  @Override
  public void close() throws Exception {
    this.client.close();
  }

  /**
   * Send signal information to zookeeper by using curator framework.
   * @param identifier an identifier of operator
   * @param signal a signal
   * @throws Exception
   */
  @Override
  public void sendSignal(final String identifier, final T signal) throws Exception {
    final Stat stat = this.client.checkExists().forPath(identifier);
    if (stat == null) {
      final String path = this.client.create().creatingParentsIfNeeded().forPath(identifier);
      LOG.log(Level.INFO, "Zookeeper path Created: " + path);
    }
    this.client.setData().forPath(identifier, encoder.encode(signal));
  }
}
