package edu.snu.org.naive;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import edu.snu.org.TopologyRunner;
import edu.snu.org.bolt.TotalRankingsBolt;
import edu.snu.org.mtss.Timescale;
import edu.snu.org.naive.bolt.WordCountByWindowBolt;
import edu.snu.org.spout.RandomWordSpout;
import edu.snu.org.util.StormRunner;

/**
 * Topology for naive word count implementation
 */
public class NaiveWordCountRunner implements TopologyRunner {

  private static final Logger LOG = Logger.getLogger(NaiveWordCountRunner.class);

  private final TopologyBuilder builder;
  private final String topologyName;
  private final Config topologyConfig;


  public NaiveWordCountRunner() throws InterruptedException {
    // Gets topology builder, topology name, and topology configuration
    builder = new TopologyBuilder();
    this.topologyName = "NaiveWordcount";
    topologyConfig = new Config();
  }

  public void wireTopology(int numSpout, int numBolt, int topN, List<Timescale> timescales, int inputInterval) {
    String spoutId = "wordGenerator";
    String counterId = "counter";
    String sorterId = "sorter";

    // Configures spout
    builder.setSpout(spoutId, new RandomWordSpout(inputInterval), numSpout);
    // Configures counting and sorting bolts in multi-time scales
    int i = 0;
    for (Timescale ts : timescales) {
      int windowSize = (int)ts.getWindowSize();
      int slideInterval = (int)ts.getIntervalSize();
      builder.setBolt(counterId + i, new WordCountByWindowBolt(windowSize, slideInterval), numBolt)
      .fieldsGrouping(spoutId, new Fields("word"));
      builder.setBolt(sorterId + i, new TotalRankingsBolt(topN, numBolt, "naive-window-" + windowSize), 1).allGrouping(counterId + i);
      
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
      List<Timescale> timescales, int runtimeInSeconds, int inputInterval)
      throws AlreadyAliveException, InvalidTopologyException {
    wireTopology(numSpout, numBolt, topN, timescales, inputInterval);
    StormRunner.runTopologyRemotely(builder.createTopology(), topologyName, conf);    
  }
}