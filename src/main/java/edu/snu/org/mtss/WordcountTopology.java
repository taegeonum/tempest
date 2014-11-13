package edu.snu.org.mtss;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import edu.snu.org.mtss.bolt.MTSWordcountBolt;
import edu.snu.org.mtss.bolt.TotalRankingsBolt;
import edu.snu.org.mtss.spout.TestSpout;
import edu.snu.org.mtss.util.StormRunner;

public class WordcountTopology {

  private static final Logger LOG = Logger.getLogger(WordcountTopology.class);
  private static final int DEFAULT_RUNTIME_IN_SECONDS = 30;
  private static final int TOP_N = 10;
  
  private static final int NUM_SPOUT = 3;
  private static final int NUM_WC_BOLT = 16;

  private final TopologyBuilder builder;
  private final String topologyName;
  private final Config topologyConfig;
  private final int runtimeInSeconds;
  private final List<Timescale> timescales;

  public WordcountTopology(String topologyName) throws Exception {
    builder = new TopologyBuilder();
    this.topologyName = topologyName;
    topologyConfig = createTopologyConfiguration();
    runtimeInSeconds = DEFAULT_RUNTIME_IN_SECONDS;

    timescales = new ArrayList<>();
    timescales.add(new Timescale(2, 1, TimeUnit.SECONDS, TimeUnit.SECONDS));
    timescales.add(new Timescale(4, 2, TimeUnit.SECONDS, TimeUnit.SECONDS));
    timescales.add(new Timescale(8, 4, TimeUnit.SECONDS, TimeUnit.SECONDS));
    timescales.add(new Timescale(16, 8, TimeUnit.SECONDS, TimeUnit.SECONDS));

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
    
    
    builder.setSpout(spoutId, new TestSpout(), NUM_SPOUT);
    builder.setBolt(counterId, new MTSWordcountBolt(timescales), NUM_WC_BOLT).fieldsGrouping(spoutId, new Fields("word"));

    int i = 0;
    for (Timescale ts : timescales) {
      builder.setBolt(totalRankerId+"-"+i, new TotalRankingsBolt(TOP_N, NUM_WC_BOLT, "TotalRanking_Size" + ts.getWindowSize())).globalGrouping(counterId, "size" + ts.getWindowSize());
      i += 1;
    }
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
      rtw.runRemotely();
    }
  }
  
  public void runLocally() throws InterruptedException {
    StormRunner.runTopologyLocally(builder.createTopology(), topologyName, topologyConfig, runtimeInSeconds);
  }
  
  public void runRemotely() throws Exception {
    StormRunner.runTopologyRemotely(builder.createTopology(), topologyName, topologyConfig);
  }
}
