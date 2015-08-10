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

import edu.snu.tempest.signal.SignalReceiverStage;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.Decoder;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

import javax.inject.Inject;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * MTSSignalReceiver implementation using Zookeeper.
 */
public final class ZkSignalReceiverStage<T> implements SignalReceiverStage<T>, Watcher {
  private static final Logger LOG = Logger.getLogger(ZkSignalReceiverStage.class.getName());
  public static final String NAMESPACE = "tempest-signal";

  /**
   * A decoder for signal.
   */
  private final Decoder<T> decoder;

  /**
   * A curator framework for receiving events from zookeeper.
   */
  private final CuratorFramework client;

  /**
   * A map of signal handler.
   */
  private final ConcurrentMap<String, EventHandler<T>> handlerMap;

  /**
   * SignalReceiver implementation using Zookeeper.
   * @param decoder a decoder for signal
   * @param address an address of zookeeper
   * @param retryTimes retry time for zookeeper connection
   * @param retryPeriod retry period for zookeeper connection
   */
  @Inject
  private ZkSignalReceiverStage(
      @Parameter(ZkMTSParameters.ZkSignalDecoder.class) final Decoder<T> decoder,
      @Parameter(ZkMTSParameters.ZkServerAddress.class) final String address,
      @Parameter(ZkMTSParameters.ZkRetryTimes.class) final int retryTimes,
      @Parameter(ZkMTSParameters.ZkRetryPeriod.class) final int retryPeriod) throws Exception {
    this.decoder = decoder;
    LOG.log(Level.INFO, "Zookeeper connection from ZkSignalReceiver: " + address);
    this.client = CuratorFrameworkFactory.builder().namespace(NAMESPACE).connectString(address)
        .retryPolicy(new RetryNTimes(retryTimes, retryPeriod)).build();
    this.client.start();
    this.handlerMap = new ConcurrentHashMap<>();
  }

  @Override
  public void registerHandler(final String identifier, final EventHandler<T> eventHandler) throws Exception {
    // register watcher
    final String zkIdentifier = "/" + identifier;
    final Stat stat = this.client.checkExists().usingWatcher(this).forPath(identifier);
    if (stat == null) {
      try {
        this.client.create().creatingParentsIfNeeded().forPath(identifier);
      } catch (final Exception e) {
        LOG.log(Level.INFO, "Zookeeper Path is already set: " + identifier);
      }
    }

    final EventHandler<T> handler = handlerMap.get(zkIdentifier);
    if (handler == null) {
      handlerMap.putIfAbsent(zkIdentifier, eventHandler);
    }
  }

  /**
   * This is zookeeper interface for retrieving events from zookeeper.
   * Notice: Zookeeper cannot process multiple concurrent events. 
   * Need to use another receiver in order to address multiple concurrent events.
   */
  @Override
  public void process(final WatchedEvent event) {
    final String identifier = event.getPath();
    try {
      this.client.checkExists().usingWatcher(this).forPath(identifier);
      LOG.log(Level.INFO, "Renewed watch for path {}", identifier);
    } catch (final Exception ex) {
      LOG.log(Level.WARNING, "Error renewing watch.", ex);
    }

    final EventHandler<T> handler = handlerMap.get(identifier);
    switch (event.getType()) {
    case NodeCreated:
      LOG.log(Level.FINE, "Node created.");
      break;
    case NodeDataChanged:
      LOG.log(Level.INFO, "Received signal. Path: " + event.getPath());
      try {
        final T signal = decoder.decode(this.client.getData().forPath(event.getPath()));
        handler.onNext(signal);
      } catch (final Exception e) {
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
