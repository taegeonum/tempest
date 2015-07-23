package org.edu.snu.tempest.operators.parameters;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

@NamedParameter(doc = "caching rate of dynamic relation cube", short_name="saving_rate", default_value = "0.0")
public final class CachingRate implements Name<Double> {}