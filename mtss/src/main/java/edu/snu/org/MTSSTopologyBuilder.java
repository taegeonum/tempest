package edu.snu.org;

import java.util.List;

import javax.inject.Inject;

import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;

import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import edu.snu.org.WordCountApp.Local;
import edu.snu.org.WordCountApp.NumBolt;
import edu.snu.org.WordCountApp.NumSpout;
import edu.snu.org.WordCountApp.TimescaleClass;
import edu.snu.org.WordCountApp.TimescaleList;
import edu.snu.org.WordCountApp.TopN;
import edu.snu.org.mtss.MTSWordcountBolt;
import edu.snu.org.util.OutputWriter;
import edu.snu.org.util.OutputWriter.OutputFilePath;
import edu.snu.org.util.Timescale;

public class MTSSTopologyBuilder implements AppTopologyBuilder {

  private final StormTopology topology;
  
  @Inject
  public MTSSTopologyBuilder(BaseRichSpout spout,
      MTSWordcountBolt wcBolt,
      @Parameter(NumSpout.class) int numSpout, 
      @Parameter(NumBolt.class) int numBolt, 
      @Parameter(TopN.class) int topN, 
      @Parameter(TimescaleList.class) TimescaleClass tclass, 
      @Parameter(OutputDir.class) String outputDir,
      @Parameter(Local.class) boolean isLocal) throws InjectionException {
    
    String spoutId = "wordGenerator";
    String counterId = "mtsOperator";
    String totalRankerId = "finalRanker";
    List<Timescale> timescales = tclass.timescales;

    TopologyBuilder builder = new TopologyBuilder();
    
    builder.setSpout(spoutId, spout, numSpout);
    builder.setBolt(counterId, wcBolt, numBolt).fieldsGrouping(spoutId, new Fields("word"));
    
    // set TotalRankingsBolts
    JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.bindNamedParameter(TopN.class, topN+"");
    cb.bindNamedParameter(NumBolt.class, numBolt+"");
    cb.bindImplementation(OutputWriter.class, ClassFactory.createOutputWriterClass(isLocal));
    
    int i = 0;
    for (Timescale ts : timescales) {
      Injector ij = Tang.Factory.getTang().newInjector(cb.build());
      ij.bindVolatileParameter(OutputFilePath.class, outputDir + "mtss-window-" + ts.windowSize + "-" + ts.intervalSize);
      TotalRankingsBolt bolt = ij.getInstance(TotalRankingsBolt.class);
      builder.setBolt(totalRankerId+"-"+i, bolt).globalGrouping(counterId, "size" + ts.windowSize);
      i += 1;
    }
    
    topology = builder.createTopology();
  }


  @Override
  public StormTopology createTopology() {
    return topology;
  }

}
