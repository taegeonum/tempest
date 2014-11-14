package edu.snu.org;

import java.util.List;

import backtype.storm.Config;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import edu.snu.org.mtss.Timescale;

public interface TopologyRunner {

  public void runLocally(Config conf, int numSpout, int numBolt, int topN, List<Timescale> timescales, int runtimeInSeconds, int inputInterval) throws InterruptedException ;
  public void runRemotely(Config conf, int numSpout, int numBolt, int topN, List<Timescale> timescales, int runtimeInSeconds, int inputInterval) throws AlreadyAliveException, InvalidTopologyException;
}
