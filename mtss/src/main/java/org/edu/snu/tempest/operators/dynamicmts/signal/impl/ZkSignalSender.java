package org.edu.snu.tempest.operators.dynamicmts.signal.impl;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.remote.Encoder;
import org.apache.zookeeper.data.Stat;
import org.edu.snu.tempest.operators.Timescale;
import org.edu.snu.tempest.operators.dynamicmts.signal.MTSSignalSender;
import org.edu.snu.tempest.operators.dynamicmts.signal.TimescaleSignal;
import org.edu.snu.tempest.operators.dynamicmts.signal.impl.ZkMTSParameters.OperatorIdentifier;
import org.edu.snu.tempest.operators.dynamicmts.signal.impl.ZkMTSParameters.ZkServerAddress;
import org.edu.snu.tempest.operators.dynamicmts.signal.impl.ZkMTSParameters.ZkTSEncoder;

import javax.inject.Inject;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Zookeeper MTSSignalSender implementation.
 * 
 */
public class ZkSignalSender implements MTSSignalSender {

  private static final Logger LOG = Logger.getLogger(ZkSignalSender.class.getName());
  
  private final CuratorFramework client;
  private final Encoder<TimescaleSignal> encoder;
  private final String identifier;
  
  @Inject
  public ZkSignalSender(
      @Parameter(ZkServerAddress.class) final String address, 
      @Parameter(OperatorIdentifier.class) final String identifier, 
      @Parameter(ZkTSEncoder.class) final Encoder<TimescaleSignal> encoder) {
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
  
  private void sendTimescaleInfo(final TimescaleSignal ts, final String type) throws Exception {
    Stat stat = this.client.checkExists().forPath(this.identifier + "-" + type);
    if (stat == null) {
      String path = this.client.create().creatingParentsIfNeeded().forPath(this.identifier + "-" + type);
      LOG.log(Level.INFO, "Zookeeper path Created: " + path);
    }
    this.client.setData().forPath(this.identifier + "-" + type, encoder.encode(ts));
  }
  
  @Override
  public void addTimescale(final Timescale ts) throws Exception {
    sendTimescaleInfo(new TimescaleSignal(ts,
        TimeUnit.NANOSECONDS.toSeconds(System.nanoTime())), "addition");
    LOG.log(Level.INFO, "Add timescale: " + ts);
  }

  @Override
  public void removeTimescale(final Timescale ts) throws Exception {
    sendTimescaleInfo(new TimescaleSignal(ts,
        TimeUnit.NANOSECONDS.toSeconds(System.nanoTime())), "deletion");
    LOG.log(Level.INFO, "Remove timescale: " + ts);
  }
}
