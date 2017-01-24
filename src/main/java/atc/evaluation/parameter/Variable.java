package atc.evaluation.parameter;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

/**
 * Created by taegeonum on 4/29/16.
 */

@NamedParameter(doc = "experiment variable", short_name = "variable")
public final class Variable implements Name<String> {}
