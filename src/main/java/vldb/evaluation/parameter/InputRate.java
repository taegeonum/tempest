package vldb.evaluation.parameter;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

/**
 * Created by taegeonum on 9/21/15.
 */
@NamedParameter(short_name="input_rate", default_value = "100000")
public final class InputRate implements Name<Long> {
}
