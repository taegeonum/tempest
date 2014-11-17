package edu.snu.org.mtss;

import java.util.List;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import edu.snu.org.TopologyRunner;
import edu.snu.org.bolt.TotalRankingsBolt;
import edu.snu.org.mtss.bolt.MTSWordcountBolt;
import edu.snu.org.spout.RandomWordSpout;
import edu.snu.org.util.StormRunner;

public class MTSSTopologyRunner implements TopologyRunner {
  private static final Logger LOG = Logger.getLogger(MTSSTopologyRunner.class);

  private final String topologyName;
  private TopologyBuilder builder;
  
  public MTSSTopologyRunner() throws Exception {
    this.topologyName = "MTSSTopology";
    this.builder = new TopologyBuilder();
  }
  
  public void wireTopology(int numSpout, int numBolt, int topN, List<Timescale> timescales, int inputInterval) {
    String spoutId = "wordGenerator";
    String counterId = "mtsOperator";
    String totalRankerId = "finalRanker";
    
    builder.setSpout(spoutId, new RandomWordSpout(inputInterval), numSpout);
    try {
      builder.setBolt(counterId, new MTSWordcountBolt(timescales), numBolt).fieldsGrouping(spoutId, new Fields("word"));
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    int i = 0;
    for (Timescale ts : timescales) {
      builder.setBolt(totalRankerId+"-"+i, new TotalRankingsBolt(topN, numBolt, "mtss-window-" + ts.getWindowSize())).globalGrouping(counterId, "size" + ts.getWindowSize());
      i += 1;
    }
  }

  @Override
  public void runLocally(Config conf, int numSpout, int numBolt, int topN,
      List<Timescale> timescales, int runtimeInSeconds, int inputInterval) throws InterruptedException {

    wireTopology(numSpout, numBolt, topN, timescales, inputInterval);
    StormRunner.runTopologyLocally(builder.createTopology(), topologyName, conf, runtimeInSeconds);

  }

  @Override
  public void runRemotely(Config conf, int numSpout, int numBolt, int topN,
      List<Timescale> timescales, int runtimeInSeconds, int inputInterval) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
    wireTopology(numSpout, numBolt, topN, timescales, inputInterval);
    StormRunner.runTopologyRemotely(builder.createTopology(), topologyName, conf);
  }
}
