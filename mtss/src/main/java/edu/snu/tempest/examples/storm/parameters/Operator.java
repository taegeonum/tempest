package edu.snu.tempest.examples.storm.parameters;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

@NamedParameter(doc = "type of mts operator", short_name="mts_operator", default_value = "dynamic_mts")
public final class Operator implements Name<String> {}
