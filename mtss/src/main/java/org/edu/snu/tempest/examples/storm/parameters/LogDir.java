package org.edu.snu.tempest.examples.storm.parameters;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

@NamedParameter(doc = "logging directory", short_name = "log_dir",
    default_value="/Users/taegeonum/Projects/CMS_SNU/BDCS/MSS/mtss/log/")
public final class LogDir implements Name<String> {}
