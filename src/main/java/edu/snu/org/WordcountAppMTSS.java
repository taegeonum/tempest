package edu.snu.org;

import java.util.List;

import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import edu.snu.org.mtss.MTSWordcountBolt;
import edu.snu.org.mtss.Timescale;
import edu.snu.org.util.StormRunner;

public class WordcountAppMTSS {

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
    
    try {
      builder.setBolt(counterId, new MTSWordcountBolt(timescales), WCConf.NUM_WC_BOLT).fieldsGrouping(spoutId, new Fields("word"));
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    int i = 0;
    for (Timescale ts : timescales) {
      builder.setBolt(totalRankerId+"-"+i, new TotalRankingsBolt(WCConf.TOP_N, WCConf.NUM_WC_BOLT, "mtss-window-" + ts.getWindowSize(), "folder")).globalGrouping(counterId, "size" + ts.getWindowSize() + "-" + ts.getIntervalSize());
      i += 1;
    }
  }

}
