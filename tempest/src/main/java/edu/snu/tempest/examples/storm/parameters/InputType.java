package edu.snu.tempest.examples.storm.parameters;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

@NamedParameter(doc = "input", short_name="input", default_value = "zipfian")
public final class InputType implements Name<String> {}
