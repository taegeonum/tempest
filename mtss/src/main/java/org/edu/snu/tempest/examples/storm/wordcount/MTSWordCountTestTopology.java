package org.edu.snu.tempest.examples.storm.wordcount;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.formats.CommandLine;
import org.edu.snu.naive.operator.impl.NaiveWindowOperator;
import org.edu.snu.tempest.examples.storm.parameters.*;
import org.edu.snu.tempest.examples.utils.StormRunner;
import org.edu.snu.tempest.examples.utils.TimescaleParser.TimescaleParameter;
import org.edu.snu.tempest.examples.utils.writer.LocalOutputWriter;
import org.edu.snu.tempest.examples.utils.writer.OutputWriter;

import java.io.IOException;
import java.util.logging.Logger;

/**
 * WordCountTest topology.
 */
public final class MTSWordCountTestTopology {
  private static final Logger LOG = Logger.getLogger(MTSWordCountTestTopology.class.getName());

  private MTSWordCountTestTopology() {

  }

  private static Configuration getCommandLineConf(String[] args) throws BindException, IOException {

    final Tang tang = Tang.Factory.getTang();
    final JavaConfigurationBuilder cb = tang.newConfigurationBuilder();

    CommandLine cl = new CommandLine(cb)
        .registerShortNameOfClass(TestName.class)
        .registerShortNameOfClass(LogDir.class)
        .registerShortNameOfClass(SavingRate.class)
        .registerShortNameOfClass(TimescaleParameter.class)
        .registerShortNameOfClass(NumSpouts.class)
        .registerShortNameOfClass(TotalTime.class)
        .registerShortNameOfClass(Operator.class)
        .registerShortNameOfClass(InputType.class)
        .processCommandLine(args);

    return cl.getBuilder().build();
  }
  
  public static void main(String[] args) throws Exception {
    final Config conf = new Config();
    conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS, "-Xms16384m -Xmx30000m");
    conf.put(Config.STORM_ZOOKEEPER_CONNECTION_TIMEOUT, 500000);
    conf.put(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT, 500000);
    
    Configuration commandLineConf = getCommandLineConf(args);
    Injector injector = Tang.Factory.getTang().newInjector(commandLineConf);
    WordCountTest test = injector.getInstance(WordCountTest.class);
    final String testName = injector.getNamedInstance(TestName.class);
    final String logDir = injector.getNamedInstance(LogDir.class);
    final double savingRate = injector.getNamedInstance(SavingRate.class);
    final String operator = injector.getNamedInstance(Operator.class);
    final String topologyName = operator + "_WC_TOPOLOGY";
    final String inputType = injector.getNamedInstance(InputType.class);
    
    TopologyBuilder builder = new TopologyBuilder();
    BaseRichSpout spout = null;
    if (inputType.compareTo("random") == 0) {
      spout = new RandomWordSpout(1);
    } else if (inputType.compareTo("zipfian") == 0) {
      spout = new ZipfianWordSpout(1);
    }
    
    OutputWriter writer = new LocalOutputWriter();
    // For logging initial configuration.
    writer.write(logDir + testName + "/conf", test.print() + "\n" + "saving: " + savingRate);
    writer.write(logDir + testName + "/initialTime", System.currentTimeMillis()+"");
    writer.write(logDir + testName + "/largestTimescale", test.tsParser.largestWindowSize()+"");
    writer.close();

    // set spout
    builder.setSpout("wcspout", spout, test.numSpouts);
    BaseRichBolt bolt = new MTSWordCountTestBolt(new LocalOutputWriter(),
        logDir + testName,
        test.tsParser.timescales,
        test.operatorClass,
        "localhost:2181",
        savingRate);

    // set bolt
    if (test.operatorClass.equals(NaiveWindowOperator.class)) {
      // naive creates a bolt with multiple executors.
      // Each executors runs one timescale window operator.
      builder.setBolt("wcbolt", bolt, test.tsParser.timescales.size()).allGrouping("wcspout");
    } else {
      builder.setBolt("wcbolt", bolt, 1).fieldsGrouping("wcspout", new Fields("word"));
    }

    final StormTopology topology = builder.createTopology();
    StormRunner.runTopologyLocally(topology, topologyName, conf, test.totalTime);
  }
}
