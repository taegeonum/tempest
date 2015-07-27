package edu.snu.tempest.examples.storm.parameters;


import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

@NamedParameter(doc = "test name", short_name = "test_name", default_value="default-test")
public final class TestName implements Name<String> {}