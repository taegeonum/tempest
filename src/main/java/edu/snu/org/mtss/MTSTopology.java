package edu.snu.org.mtss;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import edu.snu.org.mtss.bolt.MTSOperatorBolt;
import edu.snu.org.mtss.bolt.TotalRankingsBolt;
import edu.snu.org.mtss.spout.TestSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class MTSTopology {

  private static final Logger LOG = Logger.getLogger(MTSTopology.class);
  private static final int DEFAULT_RUNTIME_IN_SECONDS = 60;
  private static final int TOP_N = 5;

  private final TopologyBuilder builder;
  private final String topologyName;
  private final Config topologyConfig;
  private final int runtimeInSeconds;

  public MTSTopology(String topologyName) throws InterruptedException {
    builder = new TopologyBuilder();
    this.topologyName = topologyName;
    topologyConfig = createTopologyConfiguration();
    runtimeInSeconds = DEFAULT_RUNTIME_IN_SECONDS;

    wireTopology();
  }

  private static Config createTopologyConfiguration() {
    Config conf = new Config();
    conf.setDebug(false);
    return conf;
  }

  private void wireTopology() throws InterruptedException {
    String spoutId = "wordGenerator";
    String counterId = "mtsOperator";
    String totalRankerId = "finalRanker";
    
    Map<Integer, Integer> windowMap = new HashMap<>();
    windowMap.put(5,2);
    windowMap.put(10, 4);
    windowMap.put(20, 8);
    
    builder.setSpout(spoutId, new TestSpout(), 3);
    builder.setBolt(counterId, new MTSOperatorBolt(windowMap), 4).fieldsGrouping(spoutId, new Fields("word"));
    builder.setBolt(totalRankerId, new TotalRankingsBolt(TOP_N)).globalGrouping(counterId);
  }

  public void runLocally() throws InterruptedException {
    runTopologyLocally(builder.createTopology(), topologyName, topologyConfig, runtimeInSeconds);
  }


  public static void main(String[] args) throws Exception {
    String topologyName = "slidingWindowCounts";
    if (args.length >= 1) {
      topologyName = args[0];
    }
    boolean runLocally = true;
    if (args.length >= 2 && args[1].equalsIgnoreCase("remote")) {
      runLocally = false;
    }

    LOG.info("Topology name: " + topologyName);
    MTSTopology rtw = new MTSTopology(topologyName);
    if (runLocally) {
      LOG.info("Running in local mode");
      rtw.runLocally();
    }
    else {
      LOG.info("Running in remote (cluster) mode");
    }
  }
  
  
  public static void runTopologyLocally(StormTopology topology, String topologyName, Config conf, int runtimeInSeconds)
      throws InterruptedException {
    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology(topologyName, conf, topology);
    Thread.sleep((long) runtimeInSeconds * 1000);
    cluster.killTopology(topologyName);
    cluster.shutdown();
  }
  
}
