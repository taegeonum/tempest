package edu.snu.org;

import java.security.InvalidParameterException;

import backtype.storm.topology.base.BaseRichSpout;

public class ClassFactory {

  public static final Class<? extends AppTopologyBuilder> createTopologyBuilderClass(String appName) {
    if (appName.compareTo("MTSSTopology") == 0) {
      return MTSSTopologyBuilder.class;
    } else if (appName.compareTo("NaiveTopology") == 0) {
      return NaiveTopologyBuilder.class;
    } else {
      throw new InvalidParameterException("There is no topology builder matched with: " + appName);
    }
  }
  
  public static final Class<? extends BaseRichSpout> createSpoutClass(String spout) {
    if (spout.compareTo("FileReadWordSpout") == 0) {
      return FileReadWordSpout.class;
    } else if (spout.compareTo("RandomWordSpout") == 0) {
      return RandomWordSpout.class;
    } else {
      throw new InvalidParameterException("There is no spout matched with: " + spout);
    }
  }
}
