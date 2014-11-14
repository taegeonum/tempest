package edu.snu.org.naive;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import edu.snu.org.bolt.TotalRankingsBolt;
import edu.snu.org.naive.bolt.WordCountByWindowBolt;
import edu.snu.org.spout.RandomWordSpout;

/**
 * Topology for naive word count implementation
 */
public class NaiveWordCountTopology {

  private static final Logger LOG = Logger.getLogger(NaiveWordCountTopology.class);
  private static final int DEFAULT_RUNTIME_IN_SECONDS = 60;
  private static final int DEFAULT_SPOUT_NUMBERS = 3;
  private static final int DEFAULT_COUNTER_PARALLELISM = 4;
  private static final int DEFAULT_TIMESCALE_NUMBERS = 4;
  private static final int[] DEFAULT_WINDOW_SIZES = new int[]{2000, 4000, 8000, 16000};
  private static final int[] DEFAULT_SLIDE_INTERVALS = new int[]{1000, 2000, 4000, 8000};

  private final TopologyBuilder builder;
  private final String topologyName;
  private final Config topologyConfig;
  private final int runtimeInSeconds;
  private final int spoutNumbers;
  private final int counterParallelism;
  private final int timescaleNumbers;
  private final List<Integer> windowSizes;
  private final List<Integer> slideIntervals;

  public NaiveWordCountTopology(String topologyName) throws InterruptedException {
    // Gets topology builder, topology name, and topology configuration
    builder = new TopologyBuilder();
    this.topologyName = topologyName;
    topologyConfig = new Config();
    // Initializes word counters as default
    runtimeInSeconds = DEFAULT_RUNTIME_IN_SECONDS;
    spoutNumbers = DEFAULT_SPOUT_NUMBERS;
    counterParallelism = DEFAULT_COUNTER_PARALLELISM;
    timescaleNumbers = DEFAULT_TIMESCALE_NUMBERS;
    windowSizes = new ArrayList<>();
    for (int windowSize: DEFAULT_WINDOW_SIZES) {
      windowSizes.add(windowSize);
    }
    slideIntervals = new ArrayList<>();
    for (int slideInterval: DEFAULT_SLIDE_INTERVALS) {
      slideIntervals.add(slideInterval);
    }
    wireTopology();
  }

  private void wireTopology() {
    String spoutId = "wordGenerator";
    String counterId = "counter";
    String sorterId = "sorter";

    // Configures spout
    builder.setSpout(spoutId, new RandomWordSpout(), spoutNumbers);
    // Configures counting and sorting bolts in multi-time scales
    for (int i = 0; i < timescaleNumbers; i++) {
      int windowSize = windowSizes.get(i);
      int slideInterval = slideIntervals.get(i);
      builder.setBolt(counterId + i, new WordCountByWindowBolt(windowSize, slideInterval), counterParallelism)
          .fieldsGrouping(spoutId, new Fields("word"));
      builder.setBolt(sorterId + i, new TotalRankingsBolt(10, counterParallelism, "naive-window-" + (windowSize/1000)), 1).allGrouping(counterId + i);
    }

  }

  public void runLocally(StormTopology topology, String topologyName, Config conf,
                         int runtimeInSeconds) throws InterruptedException {
    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology(topologyName, conf, topology);
    Thread.sleep((long) runtimeInSeconds * 1000);
    cluster.killTopology(topologyName);
    cluster.shutdown();
  }

  public void run() throws InterruptedException {
    runLocally(builder.createTopology(), topologyName, topologyConfig, runtimeInSeconds);
  }

  public static void main(String[] args) throws Exception {
    String topologyName = "naiveMultiTimeScaleWindowWordCounts";
    // run NaiveWordCountTopology locally...
    NaiveWordCountTopology topology = new NaiveWordCountTopology(topologyName);
    topology.run();
  }

}