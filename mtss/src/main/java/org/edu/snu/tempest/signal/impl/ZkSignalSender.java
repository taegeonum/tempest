package org.edu.snu.tempest.signal.impl;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.remote.Encoder;
import org.apache.zookeeper.data.Stat;
import org.edu.snu.tempest.Timescale;
import org.edu.snu.tempest.signal.MTSSignalSender;
import org.edu.snu.tempest.signal.impl.ZkMTSParameters.OperatorIdentifier;
import org.edu.snu.tempest.signal.impl.ZkMTSParameters.ZkMTSNamespace;
import org.edu.snu.tempest.signal.impl.ZkMTSParameters.ZkServerAddress;
import org.edu.snu.tempest.signal.impl.ZkMTSParameters.ZkTSEncoder;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
 * Zookeeper MTSSignalSender implementation 
 * 
 */
public class ZkSignalSender implements MTSSignalSender {

  private static final Logger LOG = Logger.getLogger(ZkSignalSender.class.getName());
  
  private final CuratorFramework client;
  private final Encoder<Timescale> encoder;
  private final String identifier;
  
  @Inject
  public ZkSignalSender(
      @Parameter(ZkServerAddress.class) final String address, 
      @Parameter(OperatorIdentifier.class) final String identifier, 
      @Parameter(ZkMTSNamespace.class) final String namespace,
      @Parameter(ZkTSEncoder.class) final Encoder<Timescale> encoder) {

    LOG.log(Level.INFO, "Creates ZookeeperMTSClient");
    this.client = CuratorFrameworkFactory.builder().namespace(namespace).connectString(address)
        .retryPolicy(new RetryOneTime(500)).build();
    this.encoder = encoder;
    this.identifier = identifier;
    this.client.start();
  }

  
  @Override
  public void close() throws Exception {
    this.client.close();
  }

  
  private void sendTimescaleInfo(Timescale ts, String type) throws Exception {
    Stat stat = this.client.checkExists().forPath(this.identifier + "-" + type);
    if (stat == null) {
      String path = this.client.create().creatingParentsIfNeeded().forPath(this.identifier + "-" + type);
      LOG.log(Level.INFO, "Zookeeper path Created: " + path);
    }
    this.client.setData().forPath(this.identifier + "-" + type, encoder.encode(ts));
  }
  
  @Override
  public void addTimescale(Timescale ts) throws Exception {
    sendTimescaleInfo(ts, "addition");
    LOG.log(Level.INFO, "Add timescale: " + ts);
  }

  @Override
  public void removeTimescale(Timescale ts) throws Exception {
    sendTimescaleInfo(ts, "deletion");
    LOG.log(Level.INFO, "Remove timescale: " + ts);
  }
}
