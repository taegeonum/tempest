package org.edu.snu.tempest.examples.storm.parameters;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

@NamedParameter(doc = "total time of a test (Sec)", short_name = "total_time", default_value = "600")
public final class TotalTime implements Name<Integer> {}
