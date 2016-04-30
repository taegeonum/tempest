package vldb.evaluation.parameter;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

/**
 * Created by taegeonum on 4/29/16.
 */
@NamedParameter(short_name = "total_time")
public class EndTime implements Name<Long> {
}
