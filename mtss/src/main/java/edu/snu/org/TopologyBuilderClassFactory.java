package edu.snu.org;

import java.security.InvalidParameterException;

public class TopologyBuilderClassFactory {

  public static final Class<? extends AppTopologyBuilder> createTopologyBuilderClass(String appName) {
    if (appName.compareTo("MTSSTopology") == 0) {
      return MTSSTopologyBuilder.class;
    } else if (appName.compareTo("NaiveTopology") == 0) {
      return NaiveTopologyBuilder.class;
    } else {
      throw new InvalidParameterException("There is no topology builder matched with: " + appName);
    }
  }
}
