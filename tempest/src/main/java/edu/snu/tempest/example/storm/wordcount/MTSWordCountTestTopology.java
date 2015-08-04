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
package edu.snu.tempest.example.storm.wordcount;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import edu.snu.tempest.example.storm.parameter.*;
import edu.snu.tempest.example.storm.util.StormRunner;
import edu.snu.tempest.example.util.writer.LocalOutputWriter;
import edu.snu.tempest.example.util.writer.OutputWriter;
import edu.snu.tempest.operator.window.time.Timescale;
import edu.snu.tempest.operator.window.time.parameter.CachingRate;
import edu.snu.tempest.operator.window.time.parameter.TimescaleString;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.formats.CommandLine;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * WordCountTest topology.
 * This can run naive, static-mts, and dynamic-mts operators.
 */
final class MTSWordCountTestTopology {
  private static final Logger LOG = Logger.getLogger(MTSWordCountTestTopology.class.getName());

  private MTSWordCountTestTopology() {
  }

  /**
   * Parse command line arguments.
   */
  private static Configuration getCommandLineConf(String[] args) throws BindException, IOException {
    final Tang tang = Tang.Factory.getTang();
    final JavaConfigurationBuilder cb = tang.newConfigurationBuilder();

    final CommandLine cl = new CommandLine(cb)
        .registerShortNameOfClass(TestName.class)
        .registerShortNameOfClass(LogDir.class)
        .registerShortNameOfClass(CachingRate.class)
        .registerShortNameOfClass(TimescaleString.class)
        .registerShortNameOfClass(NumSpouts.class)
        .registerShortNameOfClass(TotalTime.class)
        .registerShortNameOfClass(OperatorType.class)
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

    // FIXME: unable to serialize error
    //conf.registerSerialization(TimeWindowOutput.class, WordCountWindowOutputSerializer.class);

    final Configuration commandLineConf = getCommandLineConf(args);
    final Injector injector = Tang.Factory.getTang().newInjector(commandLineConf);
    final WordCountTestUtil test = injector.getInstance(WordCountTestUtil.class);
    final String testName = injector.getNamedInstance(TestName.class);
    final String logDir = injector.getNamedInstance(LogDir.class);
    final double cachingRate = injector.getNamedInstance(CachingRate.class);
    final String operator = injector.getNamedInstance(OperatorType.class);
    final String topologyName = operator + "_WC_TOPOLOGY";
    final String inputType = injector.getNamedInstance(InputType.class);
    final int numBolts = injector.getNamedInstance(NumBolts.class);
    final double inputInterval = injector.getNamedInstance(InputInterval.class);

    final TopologyBuilder builder = new TopologyBuilder();
    BaseRichSpout spout = null;
    if (inputType.compareTo("random") == 0) {
      spout = new RandomWordSpout(inputInterval);
    } else if (inputType.compareTo("zipfian") == 0) {
      spout = new ZipfianWordSpout(inputInterval);
    }
    
    final OutputWriter writer = injector.getInstance(LocalOutputWriter.class);
    final List<Timescale> timescales = test.timescales;
    // For logging initial configuration.
    writer.write(logDir + testName + "/conf", test.print() + "\n" + "saving: " + cachingRate);
    writer.write(logDir + testName + "/initialTime", System.currentTimeMillis()+"");
    writer.write(logDir + testName + "/largestTimescale", timescales.get(timescales.size()-1)+"");
    writer.close();

    // set spout
    builder.setSpout("wcspout", spout, test.numSpouts);
    final BaseRichBolt bolt = new MTSWordCountTestBolt(
        logDir + testName,
        timescales,
        test.operatorName,
        "localhost:2181",
        cachingRate,
        TimeUnit.NANOSECONDS.toSeconds(System.nanoTime()));

    // set bolt
    if (test.operatorName.equals("naive")) {
      // naive creates a bolt with multiple executors.
      // Each executors runs one timescale window operator.
      builder.setBolt("wcbolt", bolt, timescales.size()).allGrouping("wcspout");
    } else {
      builder.setBolt("wcbolt", bolt, numBolts).fieldsGrouping("wcspout", new Fields("word"));
    }

    final StormTopology topology = builder.createTopology();
    StormRunner.runTopologyLocally(topology, topologyName, conf, test.totalTime);
  }
}
