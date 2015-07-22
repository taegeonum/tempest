package org.edu.snu.tempest.examples.storm.wordcount;

import org.apache.reef.tang.annotations.Parameter;
import org.edu.snu.tempest.examples.storm.parameters.*;
import org.edu.snu.tempest.examples.utils.TimescaleParser;

import javax.inject.Inject;

/**
 * Class for parsing parameters for test.
 */
public final class WordCountTest {

  public final int numSpouts;
  public final String testName;
  public final String logDir;
  public final double savingRate;
  public final int totalTime;
  public final TimescaleParser tsParser;
  public final String operatorName;
  public final String inputType;
  
  @Inject
  public WordCountTest(@Parameter(NumSpouts.class) final int numSpouts,
      @Parameter(TestName.class) final String testName,
      @Parameter(LogDir.class) final String logDir,
      @Parameter(SavingRate.class) final double savingRate,
      @Parameter(TotalTime.class) final int totalTime,
      @Parameter(Operator.class) final String operator,
      @Parameter(InputType.class) final String inputType,
      TimescaleParser tsParser) {
    this.numSpouts = numSpouts;
    this.testName = testName;
    this.logDir = logDir;
    this.savingRate = savingRate;
    this.totalTime = totalTime;
    this.tsParser = tsParser;
    this.operatorName = operator;
    this.inputType = inputType;
  }
  
  public String print() {
    StringBuilder sb = new StringBuilder();
    sb.append("TOTAL_TIME: " + totalTime +"\n"
        + "NUM_SPOUT: " + numSpouts + "\n" 
        + "TIMESCALES: " + tsParser.timescales + "\n"
        + "START_TIME: " + System.currentTimeMillis() + "\n" 
        + "SAVING_RATE: " + savingRate + "\n" 
        + "NUM_TIMESCALES: " + tsParser.timescales.size() + "\n"
        + "INPUT_TYPE: " + inputType + "\n"
        + "OPERATOR: " + operatorName);
    
    return sb.toString();
  }
}
