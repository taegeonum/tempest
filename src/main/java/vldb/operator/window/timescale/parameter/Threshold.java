package vldb.operator.window.timescale.parameter;


import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

@NamedParameter(doc = "threshold for the ratio build/comp time", short_name = "threshold", default_value = "0.05")
public final class Threshold implements Name<Double> {
}
