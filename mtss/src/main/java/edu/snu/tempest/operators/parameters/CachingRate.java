package edu.snu.tempest.operators.parameters;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

@NamedParameter(doc = "caching rate of dynamic relation cube.",
    short_name="caching_rate", default_value = "1.0")
public final class CachingRate implements Name<Double> {}