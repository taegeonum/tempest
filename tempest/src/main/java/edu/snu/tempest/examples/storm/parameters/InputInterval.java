package edu.snu.tempest.examples.storm.parameters;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

@NamedParameter(doc = "input interval (ms)", short_name = "input_interval", default_value = "10")
public final class InputInterval implements Name<Double> {}
