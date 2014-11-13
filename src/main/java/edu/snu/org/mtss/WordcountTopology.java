package edu.snu.org.mtss;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import edu.snu.org.mtss.bolt.MTSWordcountBolt;
import edu.snu.org.mtss.bolt.TotalRankingsBolt;
import edu.snu.org.mtss.spout.TestSpout;

public class WordcountTopology {

  private static final Logger LOG = Logger.getLogger(WordcountTopology.class);
  private static final int DEFAULT_RUNTIME_IN_SECONDS = 60;
  private static final int TOP_N = 10;
  
  private static final int NUM_SPOUT = 3;
  private static final int NUM_WC_BOLT = 4;

  private final TopologyBuilder builder;
  private final String topologyName;
  private final Config topologyConfig;
  private final int runtimeInSeconds;

  public WordcountTopology(String topologyName) throws Exception {
    builder = new TopologyBuilder();
    this.topologyName = topologyName;
    topologyConfig = createTopologyConfiguration();
    runtimeInSeconds = DEFAULT_RUNTIME_IN_SECONDS;

    wireTopology();
  }

  private static Config createTopologyConfiguration() {
    Config conf = new Config();
    conf.put(Config.TOPOLOGY_DEBUG, false);
    conf.setDebug(false);
    return conf;
  }

  private void wireTopology() throws Exception {
    String spoutId = "wordGenerator";
    String counterId = "mtsOperator";
    String totalRankerId = "finalRanker";
    
    List<Timescale> timescales = new ArrayList<>();
    timescales.add(new Timescale(5, 2, TimeUnit.SECONDS, TimeUnit.SECONDS));
    timescales.add(new Timescale(10, 4, TimeUnit.SECONDS, TimeUnit.SECONDS));
    timescales.add(new Timescale(20, 6, TimeUnit.SECONDS, TimeUnit.SECONDS));
    
    builder.setSpout(spoutId, new TestSpout(), NUM_SPOUT);
    builder.setBolt(counterId, new MTSWordcountBolt(timescales), NUM_WC_BOLT).fieldsGrouping(spoutId, new Fields("word"));
    builder.setBolt(totalRankerId+"1", new TotalRankingsBolt(TOP_N, NUM_WC_BOLT, "TotalRanking_Size5")).globalGrouping(counterId, "size5");
    builder.setBolt(totalRankerId+"2", new TotalRankingsBolt(TOP_N, NUM_WC_BOLT, "TotalRanking_Size10")).globalGrouping(counterId, "size10");
    builder.setBolt(totalRankerId+"3", new TotalRankingsBolt(TOP_N, NUM_WC_BOLT, "TotalRanking_Size20")).globalGrouping(counterId, "size20");
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
    WordcountTopology rtw = new WordcountTopology(topologyName);
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
