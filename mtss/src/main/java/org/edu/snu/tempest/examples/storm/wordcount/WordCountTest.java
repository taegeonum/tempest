package org.edu.snu.tempest.examples.storm.wordcount;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;
import org.edu.snu.naive.operator.impl.NaiveWindowOperator;
import org.edu.snu.onthefly.operator.impl.OTFMTSOperatorImpl;
import org.edu.snu.tempest.examples.utils.TimescaleParser;
import org.edu.snu.tempest.operator.MTSOperator;
import org.edu.snu.tempest.operator.impl.DynamicMTSOperatorImpl;
import org.edu.snu.tempest.operator.impl.StaticMTSOperatorImpl;

import javax.inject.Inject;

/**
 * Class for parsing parameters for test.
 */
public class WordCountTest {
  
  @NamedParameter(doc = "num of spouts", short_name = "spouts", default_value="30")
  public static final class NumSpouts implements Name<Integer> {}

  @NamedParameter(doc = "test name", short_name = "test_name", default_value="default-test")
  public static final class TestName implements Name<String> {}
  
  @NamedParameter(doc = "logging directory", short_name = "log_dir",
      default_value="/Users/taegeonum/Projects/CMS_SNU/BDCS/MSS/mtss/log/")
  public static final class LogDir implements Name<String> {}
  
  @NamedParameter(doc = "save percentage", short_name = "saving_rate", default_value="0.0")
  public static final class SavingRate implements Name<Double> {}
  
  @NamedParameter(doc = "total time of a test (Sec)", short_name = "total_time", default_value = "600")
  public static final class TotalTime implements Name<Integer> {}
  
  @NamedParameter(doc = "timescale addition interval (sec)", short_name = "ts_interval", default_value = "120")
  public static final class TsInterval implements Name<Integer> {}
  
  @NamedParameter(doc = "mts test", short_name="operator", default_value = "mts")
  public static final class Operator implements Name<String> {}
 
  @NamedParameter(doc = "input", short_name="input", default_value = "zipfian")
  public static final class InputType implements Name<String> {}

  @NamedParameter(doc = "input interval (ms)", short_name = "input_interval", default_value = "10")
  public static final class InputInterval implements Name<Double> {}

  public final int numSpouts;
  public final String testName;
  public final String logDir;
  public final double savingRate;
  public final int totalTime;
  public final int tsInterval;
  public final TimescaleParser tsParser;
  public final Class<? extends MTSOperator> operatorClass;
  public final String inputType;
  
  @Inject
  public WordCountTest(@Parameter(NumSpouts.class) final int numSpouts,
      @Parameter(TestName.class) final String testName,
      @Parameter(LogDir.class) final String logDir,
      @Parameter(SavingRate.class) final double savingRate,
      @Parameter(TotalTime.class) final int totalTime,
      @Parameter(TsInterval.class) final int tsInterval,
      @Parameter(Operator.class) final String operator,
      @Parameter(InputType.class) final String inputType,
      TimescaleParser tsParser) {
    this.numSpouts = numSpouts;
    this.testName = testName;
    this.logDir = logDir;
    this.savingRate = savingRate;
    this.totalTime = totalTime;
    this.tsInterval = tsInterval * 1000;
    this.tsParser = tsParser;
    this.inputType = inputType;
    
    if (operator.equals("mts")) {
      operatorClass = DynamicMTSOperatorImpl.class;
    } else if (operator.equals("naive")) { 
      operatorClass = NaiveWindowOperator.class;
    } else if (operator.equals("rg")) {
      operatorClass = StaticMTSOperatorImpl.class;
    } else if (operator.equals("otf")) {
      operatorClass = OTFMTSOperatorImpl.class;
    } else {
      throw new RuntimeException("Operator exception: " + operator);
    }
  }
  
  public final String print() {
    StringBuilder sb = new StringBuilder();
    sb.append("TOTAL_TIME: " + totalTime +"\n"
        + "NUM_SPOUT: " + numSpouts + "\n" 
        + "TIMESCALE_ADD_INTERVAL: " + tsInterval + "\n" 
        + "TIMESCALES: " + tsParser.timescales + "\n"
        + "START_TIME: " + System.currentTimeMillis() + "\n" 
        + "SAVING_RATE: " + savingRate + "\n" 
        + "NUM_TIMESCALES: " + tsParser.timescales.size() + "\n"
        + "INPUT_TYPE: " + inputType + "\n"
        + "OPERATOR: " + operatorClass.getName());
    
    return sb.toString();
  }
}
