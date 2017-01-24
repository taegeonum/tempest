package atc.operator.window.timescale.parameter;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

/**
 * GCD of all window sizes.
 */
@NamedParameter(default_value = "1")
public class GCD implements Name<Long> {
}
