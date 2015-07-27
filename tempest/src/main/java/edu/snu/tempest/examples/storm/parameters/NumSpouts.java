package edu.snu.tempest.examples.storm.parameters;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;


@NamedParameter(doc = "num of spouts", short_name = "spouts", default_value="30")
public final class NumSpouts implements Name<Integer> {}

