package evaluation.example.parameter;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

/**
 * Created by taegeonum on 9/21/15.
 */
@NamedParameter(short_name = "num_keys", default_value = "100000")
public final class NumOfKey implements Name<Long> {
}
