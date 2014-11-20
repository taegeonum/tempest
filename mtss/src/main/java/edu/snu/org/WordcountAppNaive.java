package edu.snu.org;

import java.util.List;

import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import edu.snu.org.naive.WordCountByWindowBolt;
import edu.snu.org.util.StormRunner;
import edu.snu.org.util.Timescale;

public class WordcountAppNaive {


  private final static TopologyBuilder builder = new TopologyBuilder();

  public static void main(String[] args) throws InterruptedException, AlreadyAliveException, InvalidTopologyException, AuthorizationException {
    String topologyName = "slidingWindowCounts";
    if (args.length >= 1) {
      topologyName = args[0];
    }
    boolean runLocally = true;
    
    if (args.length >= 2 && args[1].equalsIgnoreCase("remote")) {
      runLocally = false;
    }
    
    wireTopology();
    
    if (runLocally) {
      StormRunner.runTopologyLocally(builder.createTopology(), topologyName, WCConf.createTopologyConfiguration(), WCConf.DEFAULT_RUNTIME_IN_SECONDS);
    }
    else {
      StormRunner.runTopologyRemotely(builder.createTopology(), topologyName, WCConf.createTopologyConfiguration());
    }
    
  }
  
  public static void wireTopology() {
    String spoutId = "wordGenerator";
    String counterId = "mtsOperator";
    String totalRankerId = "finalRanker";
    
    builder.setSpout(spoutId, new RandomWordSpout(WCConf.INPUT_INTERVAL), WCConf.NUM_SPOUT);
    List<Timescale> timescales = WCConf.timescales();
    
    int i = 0;
    for (Timescale ts : timescales) {
      int windowSize = (int)ts.getWindowSize();
      int slideInterval = (int)ts.getIntervalSize();
      builder.setBolt(counterId + i, new WordCountByWindowBolt(windowSize, slideInterval), WCConf.NUM_WC_BOLT)
      .fieldsGrouping(spoutId, new Fields("word"));
      builder.setBolt(totalRankerId + i, new TotalRankingsBolt(WCConf.TOP_N, WCConf.NUM_WC_BOLT, "naive-window-" + windowSize + "-" + slideInterval, "folder"), 1).allGrouping(counterId + i);
      
      i += 1;
    }
  }

}