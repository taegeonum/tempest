package vldb.operator.window.timescale.parameter;


import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

@NamedParameter(doc = "selection decision period", short_name = "decision_period", default_value = "1000")
public final class DecisionPeriod implements Name<Long> {
}
