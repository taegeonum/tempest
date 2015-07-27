package edu.snu.tempest.examples.storm.parameters;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

@NamedParameter(doc = "logging directory", short_name = "log_dir")
public final class LogDir implements Name<String> {}
