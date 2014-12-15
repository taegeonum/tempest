package edu.snu.org.UniqWordCount;

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
import edu.snu.org.AppTopologyBuilder;
import edu.snu.org.ClassFactory;
import edu.snu.org.AppTopologyBuilder.OutputDir;
import edu.snu.org.TestApp.Local;
import edu.snu.org.TestApp.NumBolt;
import edu.snu.org.TestApp.NumSpout;
import edu.snu.org.TestApp.TimescaleClass;
import edu.snu.org.TestApp.TimescaleList;
import edu.snu.org.TestApp.TopN;
import edu.snu.org.grouping.WordGroupingMergeBolt;
import edu.snu.org.grouping.WordGroupingNaiveWindowBolt;
import edu.snu.org.util.OutputWriter;
import edu.snu.org.util.Timescale;
import edu.snu.org.util.OutputWriter.OutputFilePath;
import edu.snu.org.wordcount.WindowBoltParameter.SlideInterval;
import edu.snu.org.wordcount.WindowBoltParameter.WindowLength;

public class UniqWordCountNaiveBuilder implements AppTopologyBuilder {
  private final StormTopology topology;
  
  @Inject
  public UniqWordCountNaiveBuilder(BaseRichSpout spout,
      @Parameter(NumSpout.class) int numSpout, 
      @Parameter(NumBolt.class) int numBolt, 
      @Parameter(TopN.class) int topN, 
      @Parameter(TimescaleList.class) TimescaleClass tclass, 
      @Parameter(OutputDir.class) String outputDir, 
      @Parameter(Local.class) boolean isLocal) throws InjectionException {
    
    String spoutId = "wordGenerator";
    String counterId = "UniqWordcountNaiveWindowOperator";
    String totalRankerId = "finalRanker";
    List<Timescale> timescales = tclass.timescales;

    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout(spoutId, spout, numSpout);

    JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.bindNamedParameter(TopN.class, topN+"");
    cb.bindNamedParameter(NumBolt.class, numBolt+"");
    cb.bindImplementation(OutputWriter.class, ClassFactory.createOutputWriterClass(isLocal));
    
    int i = 0;
    for (Timescale ts : timescales) {
      int windowSize = (int)ts.windowSize;
      int slideInterval = (int)ts.intervalSize;
      
      JavaConfigurationBuilder childConfig = Tang.Factory.getTang().newConfigurationBuilder();
      childConfig.bindNamedParameter(OutputFilePath.class, outputDir + "naive-window-" + windowSize + "-" + slideInterval);
      childConfig.bindNamedParameter(WindowLength.class, windowSize+"");
      childConfig.bindNamedParameter(SlideInterval.class, slideInterval+"");
      Injector ij = Tang.Factory.getTang().newInjector(cb.build(), childConfig.build());  // use all these configs together
      
      UniqWordCountMergeBolt mergeBolt = ij.getInstance(UniqWordCountMergeBolt.class);
      
      builder.setBolt(counterId + i, ij.getInstance(UniqWordCountNaiveWindowBolt.class), numBolt)
      .fieldsGrouping(spoutId, new Fields("word"));
      builder.setBolt(totalRankerId + i, mergeBolt, 1).allGrouping(counterId + i);
      i += 1;
    }
    
    topology = builder.createTopology();
  }

  @Override
  public StormTopology createTopology() {
    return topology;
  }
}
