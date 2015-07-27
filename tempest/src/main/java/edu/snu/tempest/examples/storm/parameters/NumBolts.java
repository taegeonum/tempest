package edu.snu.tempest.examples.storm.parameters;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

@NamedParameter(doc = "num of bolts", short_name = "bolts", default_value="1")
public final class NumBolts implements Name<Integer> {}

