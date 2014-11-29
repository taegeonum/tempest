package edu.snu.org;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

import backtype.storm.generated.StormTopology;

public interface AppTopologyBuilder {
  @NamedParameter(doc = "timestamp appended directory. 1112321-dir")
  public static final class OutputDir implements Name<String> {}
  
  public StormTopology createTopology();
}
