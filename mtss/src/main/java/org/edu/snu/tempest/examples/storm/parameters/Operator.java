package org.edu.snu.tempest.examples.storm.parameters;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

@NamedParameter(doc = "mts test", short_name="operator", default_value = "mts")
public final class Operator implements Name<String> {}
