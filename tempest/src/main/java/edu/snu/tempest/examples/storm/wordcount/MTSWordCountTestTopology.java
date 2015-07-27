/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package edu.snu.tempest.examples.storm.wordcount;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import edu.snu.tempest.examples.storm.parameters.*;
import edu.snu.tempest.examples.utils.StormRunner;
import edu.snu.tempest.examples.utils.TimescaleParser.TimescaleParameter;
import edu.snu.tempest.examples.utils.writer.LocalOutputWriter;
import edu.snu.tempest.examples.utils.writer.OutputWriter;
import edu.snu.tempest.operators.parameters.CachingRate;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.formats.CommandLine;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * WordCountTest topology.
 * Test naive, on-the-fly, static-mts, and dynamic-mts operators.
 */
public final class MTSWordCountTestTopology {
  private static final Logger LOG = Logger.getLogger(MTSWordCountTestTopology.class.getName());

  private MTSWordCountTestTopology() {

  }

  private static Configuration getCommandLineConf(String[] args) throws BindException, IOException {
    final Tang tang = Tang.Factory.getTang();
    final JavaConfigurationBuilder cb = tang.newConfigurationBuilder();

    final CommandLine cl = new CommandLine(cb)
        .registerShortNameOfClass(TestName.class)
        .registerShortNameOfClass(LogDir.class)
        .registerShortNameOfClass(CachingRate.class)
        .registerShortNameOfClass(TimescaleParameter.class)
        .registerShortNameOfClass(NumSpouts.class)
        .registerShortNameOfClass(TotalTime.class)
        .registerShortNameOfClass(Operator.class)
        .registerShortNameOfClass(InputType.class)
        .registerShortNameOfClass(NumBolts.class)
        .processCommandLine(args);

    return cl.getBuilder().build();
  }
  
  public static void main(String[] args) throws Exception {
    final Config conf = new Config();
    conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS, "-Xms16384m -Xmx30000m");
    conf.put(Config.STORM_ZOOKEEPER_CONNECTION_TIMEOUT, 500000);
    conf.put(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT, 500000);
    
    final Configuration commandLineConf = getCommandLineConf(args);
    final Injector injector = Tang.Factory.getTang().newInjector(commandLineConf);
    final WordCountTest test = injector.getInstance(WordCountTest.class);
    final String testName = injector.getNamedInstance(TestName.class);
    final String logDir = injector.getNamedInstance(LogDir.class);
    final double cachingRate = injector.getNamedInstance(CachingRate.class);
    final String operator = injector.getNamedInstance(Operator.class);
    final String topologyName = operator + "_WC_TOPOLOGY";
    final String inputType = injector.getNamedInstance(InputType.class);
    final int numBolts = injector.getNamedInstance(NumBolts.class);
    
    final TopologyBuilder builder = new TopologyBuilder();
    BaseRichSpout spout = null;
    if (inputType.compareTo("random") == 0) {
      spout = new RandomWordSpout(1);
    } else if (inputType.compareTo("zipfian") == 0) {
      spout = new ZipfianWordSpout(1);
    }
    
    final OutputWriter writer = new LocalOutputWriter();
    // For logging initial configuration.
    writer.write(logDir + testName + "/conf", test.print() + "\n" + "saving: " + cachingRate);
    writer.write(logDir + testName + "/initialTime", System.currentTimeMillis()+"");
    writer.write(logDir + testName + "/largestTimescale", test.tsParser.largestWindowSize()+"");
    writer.close();

    // set spout
    builder.setSpout("wcspout", spout, test.numSpouts);
    final BaseRichBolt bolt = new MTSWordCountTestBolt(new LocalOutputWriter(),
        logDir + testName,
        test.tsParser.timescales,
        test.operatorName,
        "localhost:2181",
        cachingRate,
        TimeUnit.NANOSECONDS.toSeconds(System.nanoTime()));

    // set bolt
    if (test.operatorName.equals("naive")) {
      // naive creates a bolt with multiple executors.
      // Each executors runs one timescale window operator.
      builder.setBolt("wcbolt", bolt, test.tsParser.timescales.size()).allGrouping("wcspout");
    } else {
      builder.setBolt("wcbolt", bolt, numBolts).fieldsGrouping("wcspout", new Fields("word"));
    }

    final StormTopology topology = builder.createTopology();
    StormRunner.runTopologyLocally(topology, topologyName, conf, test.totalTime);
  }
}
