package edu.snu.org;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

import backtype.storm.generated.StormTopology;
import backtype.storm.topology.base.BaseRichSpout;

public interface AppTopologyBuilder {
  
  @NamedParameter(doc = "spout", default_class = RandomWordSpout.class)
  public static final class SpoutParameter implements Name<BaseRichSpout> {}

  public StormTopology createTopology();
}
