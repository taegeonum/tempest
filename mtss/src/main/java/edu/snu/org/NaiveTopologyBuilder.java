package edu.snu.org;

import java.util.List;

import javax.inject.Inject;

import org.apache.reef.tang.annotations.Parameter;

import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import edu.snu.org.WordCountApp.InputInterval;
import edu.snu.org.WordCountApp.NumBolt;
import edu.snu.org.WordCountApp.NumSpout;
import edu.snu.org.WordCountApp.OutputDir;
import edu.snu.org.WordCountApp.TimescaleClass;
import edu.snu.org.WordCountApp.TimescaleList;
import edu.snu.org.WordCountApp.TopN;
import edu.snu.org.naive.WordCountByWindowBolt;
import edu.snu.org.util.Timescale;

public class NaiveTopologyBuilder implements AppTopologyBuilder {


  private final StormTopology topology;
  
  @Inject
  public NaiveTopologyBuilder(BaseRichSpout spout,
      @Parameter(NumSpout.class) int numSpout, 
      @Parameter(NumBolt.class) int numBolt, 
      @Parameter(TopN.class) int topN, 
      @Parameter(TimescaleList.class) TimescaleClass tclass, 
      @Parameter(OutputDir.class) String outputDir) {

    String spoutId = "wordGenerator";
    String counterId = "mtsOperator";
    String totalRankerId = "finalRanker";
    List<Timescale> timescales = tclass.timescales;

    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout(spoutId, spout, numSpout);

    int i = 0;
    for (Timescale ts : timescales) {
      int windowSize = (int)ts.windowSize;
      int slideInterval = (int)ts.intervalSize;
      builder.setBolt(counterId + i, new WordCountByWindowBolt(windowSize, slideInterval), numBolt)
      .fieldsGrouping(spoutId, new Fields("word"));
      builder.setBolt(totalRankerId + i, new TotalRankingsBolt(topN, numBolt, "naive-window-" + windowSize + "-" + slideInterval, outputDir), 1).allGrouping(counterId + i);

      i += 1;
    }
    
    topology = builder.createTopology();
  }

  @Override
  public StormTopology createTopology() {
    return topology;
  }

}