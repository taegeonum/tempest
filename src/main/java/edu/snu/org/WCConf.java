package edu.snu.org;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import edu.snu.org.mtss.Timescale;
import backtype.storm.Config;

public class WCConf {

  public static final int DEFAULT_RUNTIME_IN_SECONDS = 30 * 60;
  public static final int TOP_N = 10;
  
  public static final int NUM_SPOUT = 8;
  public static final int NUM_WC_BOLT = 4;
  
  public static final int INPUT_INTERVAL = 5;
  public static final int NUM_WORKERS = 1;
  
  public static Config createTopologyConfiguration() {
    Config conf = new Config();
    conf.put(Config.TOPOLOGY_DEBUG, false);
    conf.setNumWorkers(NUM_WORKERS);
    conf.setNumAckers(NUM_WORKERS);
    conf.setDebug(false);
    return conf;
  }

  public static List<Timescale> timescales() {
    List<Timescale> timescales = new ArrayList<>();

    timescales.add(new Timescale(30, 2, TimeUnit.SECONDS, TimeUnit.SECONDS));
    timescales.add(new Timescale(60, 5, TimeUnit.SECONDS, TimeUnit.SECONDS));
    timescales.add(new Timescale(90, 6, TimeUnit.SECONDS, TimeUnit.SECONDS));
    timescales.add(new Timescale(120, 10, TimeUnit.SECONDS, TimeUnit.SECONDS));
    timescales.add(new Timescale(180, 12, TimeUnit.SECONDS, TimeUnit.SECONDS));
    timescales.add(new Timescale(210, 15, TimeUnit.SECONDS, TimeUnit.SECONDS));
    timescales.add(new Timescale(300, 20, TimeUnit.SECONDS, TimeUnit.SECONDS));
    
    
    return timescales;
  }
}
