package atc.evaluation.parameter;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

@NamedParameter(short_name = "ts_add_interval")
public final class TimescaleAddInterval implements Name<Integer> {
}
