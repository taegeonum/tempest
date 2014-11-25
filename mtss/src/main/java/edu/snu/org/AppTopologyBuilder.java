package edu.snu.org;

import backtype.storm.generated.StormTopology;

public interface AppTopologyBuilder {
  
  public StormTopology createTopology();
}
