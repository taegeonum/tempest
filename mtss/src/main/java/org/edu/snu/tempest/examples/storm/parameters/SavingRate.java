package org.edu.snu.tempest.examples.storm.parameters;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

@NamedParameter(doc = "save percentage", short_name = "saving_rate", default_value="0.0")
public final class SavingRate implements Name<Double> {}
