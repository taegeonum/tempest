package atc.evaluation.parameter;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

/**
 * Created by taegeonum on 4/29/16.
 */
@NamedParameter(doc = "logging directory", short_name = "output_path")
public final class OutputPath implements Name<String> {}
