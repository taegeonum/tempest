package edu.snu.org;

import backtype.storm.Config;
import edu.snu.org.mtss.Timescale;
import edu.snu.org.naive.NaiveWordCountRunner;
import edu.snu.org.util.HDFSWriter;

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class WordcountAppNaive {

  private static final Logger LOG = Logger.getLogger(WordcountAppNaive.class);
  private static final int DEFAULT_RUNTIME_IN_SECONDS = 18000;
  private static final int TOP_N = 10;

  private static final int NUM_SPOUT = 16;
  private static final int NUM_WC_BOLT = 4;

  private static final int INPUT_INTERVAL = 1;
  private static final int NUM_WORKERS = 1;

  private final String topologyName;

  private static TopologyRunner runner;

  public WordcountAppNaive(String topologyName) throws Exception {
    this.topologyName = topologyName;


  }

  private static Config createTopologyConfiguration() {
    Config conf = new Config();
    conf.put(Config.TOPOLOGY_DEBUG, false);
    conf.setNumWorkers(NUM_WORKERS);
    conf.setNumAckers(NUM_WORKERS);
    conf.setDebug(false);
    return conf;
  }


  public static void main(String[] args) throws Exception {
    String topologyName = "slidingWindowCounts";
    if (args.length >= 1) {
      topologyName = args[0];
    }
    boolean runLocally = true;
    
    if (args.length >= 2 && args[1].equalsIgnoreCase("remote")) {
      runLocally = false;
    }

    LOG.info("Topology name: " + topologyName);
    runner = new NaiveWordCountRunner();
    
    List<Timescale> timescales = new ArrayList<>();

    timescales.add(new Timescale(30, 2, TimeUnit.SECONDS, TimeUnit.SECONDS));
    timescales.add(new Timescale(60, 5, TimeUnit.SECONDS, TimeUnit.SECONDS));
    timescales.add(new Timescale(90, 6, TimeUnit.SECONDS, TimeUnit.SECONDS));
    timescales.add(new Timescale(120, 10, TimeUnit.SECONDS, TimeUnit.SECONDS));
    timescales.add(new Timescale(210, 15, TimeUnit.SECONDS, TimeUnit.SECONDS));
    timescales.add(new Timescale(300, 20, TimeUnit.SECONDS, TimeUnit.SECONDS));
    timescales.add(new Timescale(600, 30, TimeUnit.SECONDS, TimeUnit.SECONDS));

    long timestamp = System.currentTimeMillis();
    String folderName = "/storm-result-" + timestamp;
    
    /*
    HDFSWriter writer = new HDFSWriter(folderName + "/configuration");
    writer.writeLine("NUM_SPOUT: " + NUM_SPOUT);
    writer.writeLine("NUM_WC_BOLT: " + NUM_WC_BOLT);
    writer.writeLine("INPUT_INTERVAL: " + INPUT_INTERVAL);
    writer.writeLine("NUM_WORKERS: " + NUM_WORKERS);
    writer.writeLine("Timescales: " + timescales.size());
    for (Timescale timescale : timescales) {
      writer.writeLine(timescale.toString());
    }

    writer.close();
    */
    
    if (runLocally) {
      LOG.info("Running in local mode");
      runner.runLocally(createTopologyConfiguration(), NUM_SPOUT, NUM_WC_BOLT, TOP_N, timescales, DEFAULT_RUNTIME_IN_SECONDS, INPUT_INTERVAL, folderName);
    }
    else {
      LOG.info("Running in remote (cluster) mode");
      runner.runRemotely(createTopologyConfiguration(), NUM_SPOUT, NUM_WC_BOLT, TOP_N, timescales, DEFAULT_RUNTIME_IN_SECONDS, INPUT_INTERVAL, folderName);
    }
  }
}
