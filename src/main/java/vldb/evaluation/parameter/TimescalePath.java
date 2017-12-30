package vldb.evaluation.parameter;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

/**
 * Created by taegeonum on 4/29/16.
 */
@NamedParameter(doc = "ts path", short_name = "ts_path")
public final class TimescalePath implements Name<String> {}
