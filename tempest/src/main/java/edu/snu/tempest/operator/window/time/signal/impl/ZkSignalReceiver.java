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

import edu.snu.tempest.operator.window.time.signal.TimescaleSignal;
import edu.snu.tempest.operator.window.time.signal.TimescaleSignalListener;
import edu.snu.tempest.operator.window.time.signal.MTSSignalReceiver;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.remote.Decoder;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * MTSSignalReceiver implementation using Zookeeper.
 */
public final class ZkSignalReceiver implements MTSSignalReceiver, Watcher {
  private static final Logger LOG = Logger.getLogger(ZkSignalReceiver.class.getName());
  public static final String NAMESPACE = "tempest-signal";

  /**
   * An identifier for mts operator.
   */
  private final String identifier;

  /**
   * A decoder for TimescaleSignal.
   */
  private final Decoder<TimescaleSignal> decoder;

  /**
   * An address of Zookeeper.
   */
  private final String address;

  /**
   * Retry time for zookeeper connection.
   */
  private final int retryTimes;

  /**
   * Retry period for zookeeper connection.
   */
  private final int retryPeriod;

  /**
   * A curator framework for receiving events from zookeeper.
   */
  private CuratorFramework client;

  /**
   * A timescale signal listener for receiving timescale addition/deletion.
   */
  private TimescaleSignalListener listener;

  /**
   * MTSSignalReceiver implementation using Zookeeper.
   * @param identifier an mts operator identifier
   * @param decoder a decoder for TimescaleSignal
   * @param address an address of zookeeper
   * @param retryTimes retry time for zookeeper connection
   * @param retryPeriod retry period for zookeeper connection
   */
  @Inject
  private ZkSignalReceiver(
      @Parameter(ZkMTSParameters.OperatorIdentifier.class) final String identifier,
      @Parameter(ZkMTSParameters.ZkTSDecoder.class) Decoder<TimescaleSignal> decoder,
      @Parameter(ZkMTSParameters.ZkServerAddress.class) final String address,
      @Parameter(ZkMTSParameters.ZkRetryTimes.class) final int retryTimes,
      @Parameter(ZkMTSParameters.ZkRetryPeriod.class) final int retryPeriod) {
    this.identifier = identifier;
    this.decoder = decoder;
    this.address = address;
    this.retryTimes = retryTimes;
    this.retryPeriod = retryPeriod;
  }

  /**
   * Sends timescale information to TimescaleSignalListener.
   * @param tsListener timescale signal listener
   */
  @Override
  public void addTimescaleSignalListener(final TimescaleSignalListener tsListener) {
    this.listener = tsListener;
  }

  /**
   * Start curator framework.
   * @throws Exception
   */
  @Override
  public void start() throws Exception {
    if (this.listener == null) {
      throw new RuntimeException("TimescaleSignalListener is null");
    }

    LOG.log(Level.INFO, "Zookeeper connection from ZkSignalReceiver: " + address);
    this.client = CuratorFrameworkFactory.builder().namespace(NAMESPACE).connectString(address)
            .retryPolicy(new RetryNTimes(retryTimes, retryPeriod)).build();
    this.client.start();
    
    // addition watcher 
    Stat stat = this.client.checkExists().usingWatcher(this).forPath(this.identifier + "-addition");
    if (stat == null) {
      try {
        this.client.create().creatingParentsIfNeeded().forPath(this.identifier + "-addition");
      } catch (Exception e) {
        LOG.log(Level.INFO, "Zookeeper Path is already set: " + this.identifier + "-addition");
      }
    }
    
    // deletion watcher
    stat = this.client.checkExists().usingWatcher(this).forPath(this.identifier + "-deletion");
    if (stat == null) {
      try {
        this.client.create().creatingParentsIfNeeded().forPath(this.identifier + "-deletion");
      } catch (Exception e) {
        LOG.log(Level.INFO, "Zookeeper Path is already set: " + this.identifier + "-deletion");
      }
    }
  }

  /**
   * This is zookeeper interface for retrieving events from zookeeper.
   * Notice: Zookeeper cannot process multiple concurrent events. 
   * Need to use another receiver in order to address multiple concurrent events.
   */
  @Override
  public void process(final WatchedEvent event) {
    try {
      this.client.checkExists().usingWatcher(this).forPath(this.identifier + "-addition");
      this.client.checkExists().usingWatcher(this).forPath(this.identifier + "-deletion");
      LOG.log(Level.INFO, "Renewed watch for path {}", this.identifier);
    } catch (Exception ex) {
      LOG.log(Level.WARNING, "Error renewing watch.", ex);
    }

    switch (event.getType()) {
    case NodeCreated:
      LOG.log(Level.FINE, "Node created.");
      break;
    case NodeDataChanged:
      LOG.log(Level.INFO, "Received signal. Path: " + event.getPath());
      TimescaleSignal signal = null;
      try {
        signal = decoder.decode(this.client.getData().forPath(event.getPath()));
        if (event.getPath().matches("(.*)-addition")) {
          LOG.log(Level.INFO, "call onTimescaleAddition: " + signal);
          this.listener.onTimescaleAddition(signal.ts, signal.startTime);
        } else if (event.getPath().matches("(.*)-deletion")) {
          LOG.log(Level.INFO, "call onTimescaleDeletion: " + signal);
          this.listener.onTimescaleDeletion(signal.ts, signal.startTime);
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
      break;
    case NodeDeleted:
      LOG.log(Level.FINE, "NodeDeleted");
      break;
    default:
      throw new RuntimeException("Unknown type: " + event);
    }
  }
 
  @Override
  public void close() {
    this.client.close();
  }
}
