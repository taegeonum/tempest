package edu.snu.org;

import backtype.storm.Config;
import edu.snu.org.mtss.Timescale;
import edu.snu.org.naive.NaiveWordCountRunner;
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
    timescales.add(new Timescale(2, 1, TimeUnit.SECONDS, TimeUnit.SECONDS));
    timescales.add(new Timescale(4, 2, TimeUnit.SECONDS, TimeUnit.SECONDS));
    timescales.add(new Timescale(8, 4, TimeUnit.SECONDS, TimeUnit.SECONDS));
    timescales.add(new Timescale(16, 8, TimeUnit.SECONDS, TimeUnit.SECONDS));
    timescales.add(new Timescale(32, 16, TimeUnit.SECONDS, TimeUnit.SECONDS));
    timescales.add(new Timescale(64, 32, TimeUnit.SECONDS, TimeUnit.SECONDS));
    timescales.add(new Timescale(128, 64, TimeUnit.SECONDS, TimeUnit.SECONDS));
    
    if (runLocally) {
      LOG.info("Running in local mode");
      runner.runLocally(createTopologyConfiguration(), NUM_SPOUT, NUM_WC_BOLT, TOP_N, timescales, DEFAULT_RUNTIME_IN_SECONDS, INPUT_INTERVAL);
    }
    else {
      LOG.info("Running in remote (cluster) mode");
      runner.runRemotely(createTopologyConfiguration(), NUM_SPOUT, NUM_WC_BOLT, TOP_N, timescales, DEFAULT_RUNTIME_IN_SECONDS, INPUT_INTERVAL);
    }
  }
}
