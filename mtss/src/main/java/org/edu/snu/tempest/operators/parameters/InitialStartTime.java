package org.edu.snu.tempest.operators.parameters;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

@NamedParameter(doc = "initial start time of MTS operator", short_name = "start_time")
public final class InitialStartTime implements Name<Long> {}
