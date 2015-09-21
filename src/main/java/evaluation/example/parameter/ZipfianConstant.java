package evaluation.example.parameter;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

/**
 * Created by taegeonum on 9/21/15.
 */
@NamedParameter(short_name = "zipfian_constant", default_value ="0.99")
public final class ZipfianConstant implements Name<Double> {
}
