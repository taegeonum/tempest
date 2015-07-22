package org.edu.snu.tempest.signal.impl;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.remote.Decoder;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.edu.snu.tempest.signal.MTSSignalReceiver;
import org.edu.snu.tempest.signal.TimescaleSignal;
import org.edu.snu.tempest.signal.TimescaleSignalListener;
import org.edu.snu.tempest.signal.impl.ZkMTSParameters.*;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Zookeeper MTSSignalReceiver implementation.
 */
public class ZkSignalReceiver implements MTSSignalReceiver, Watcher {
  private static final Logger LOG = Logger.getLogger(ZkSignalReceiver.class.getName());
  private final String identifier;
  private final String namespace;
  private final Decoder<TimescaleSignal> decoder;
  private final String address;
  private final int retryTimes;
  private final int retryPeriod;
  
  private CuratorFramework client;
  private final TimescaleSignalListener listener;
  
  
  @Inject
  public ZkSignalReceiver(
      @Parameter(OperatorIdentifier.class) final String identifier, 
      @Parameter(ZkMTSNamespace.class) final String namespace, 
      @Parameter(ZkTSDecoder.class) Decoder<TimescaleSignal> decoder,
      @Parameter(ZkServerAddress.class) final String address,
      @Parameter(ZkRetryTimes.class) final int retryTimes,
      @Parameter(ZkRetryPeriod.class) final int retryPeriod, 
      TimescaleSignalListener listener) throws Exception {
    this.identifier = identifier;
    this.namespace = namespace; 
    this.decoder = decoder;
    this.address = address;
    this.retryTimes = retryTimes;
    this.retryPeriod = retryPeriod;
    this.listener = listener;
    
  }
  
  @Override
  public void start() throws Exception {
    LOG.log(Level.INFO, "Zookeeper connection from ZkSignalReceiver: " + address);
    this.client = CuratorFrameworkFactory.builder().namespace(namespace).connectString(address)
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
   * Notice: Zookeeper cannot process multiple concurrent events. 
   * Need to use another receiver if you want to address multiple concurrent events.
   */
  @Override
  public void process(WatchedEvent event) {
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
          this.listener.onTimescaleDeletion(signal.ts);
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
