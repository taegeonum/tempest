package edu.snu.tempest.examples.utils;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;

public final class StormRunner {

  private static final int MILLIS_IN_SEC = 1000;

  private StormRunner() {
  }

  public static void runTopologyLocally(final StormTopology topology,
                                        final String topologyName,
                                        final Config conf,
                                        final int runtimeInSeconds)
      throws InterruptedException {
    final LocalCluster cluster = new LocalCluster();
    cluster.submitTopology(topologyName, conf, topology);
    Thread.sleep((long) runtimeInSeconds * MILLIS_IN_SEC);
    cluster.killTopology(topologyName);
    Thread.sleep(20000);
    cluster.shutdown();
  }

  public static void runTopologyRemotely(final StormTopology topology,
                                         final String topologyName,
                                         final Config conf)
      throws AlreadyAliveException, InvalidTopologyException {
    StormSubmitter.submitTopology(topologyName, conf, topology);
  }
}
