/*
 * Copyright (C) 2015 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.tempest.example.storm.wordcount;

import edu.snu.tempest.operator.window.timescale.Timescale;
import edu.snu.tempest.operator.window.timescale.impl.TimescaleParser;
import atc.operator.window.timescale.parameter.CachingProb;
import atc.operator.window.timescale.parameter.TimescaleString;
import org.apache.reef.tang.annotations.Parameter;
import atc.evaluation.parameter.*;

import javax.inject.Inject;
import java.util.List;

/**
 * Class for parsing parameters for test.
 */
final class WordCountTestUtil {
  public final int numSpouts;
  public final String testName;
  public final String logDir;
  public final double cachingProb;
  public final int totalTime;
  public final String operatorName;
  public final String inputType;
  public final List<Timescale> timescales;
  
  @Inject
  private WordCountTestUtil(@Parameter(NumSpouts.class) final int numSpouts,
                            @Parameter(TestName.class) final String testName,
                            @Parameter(LogDir.class) final String logDir,
                            @Parameter(CachingProb.class) final double cachingProb,
                            @Parameter(TotalTime.class) final int totalTime,
                            @Parameter(OperatorTypeParam.class) final String operator,
                            @Parameter(InputType.class) final String inputType,
                            @Parameter(TimescaleString.class) final String timescaleParameter) {
    this.numSpouts = numSpouts;
    this.testName = testName;
    this.logDir = logDir;
    this.cachingProb = cachingProb;
    this.totalTime = totalTime;
    this.timescales = TimescaleParser.parseFromString(timescaleParameter);
    this.operatorName = operator;
    this.inputType = inputType;
  }
  
  public String print() {
    StringBuilder sb = new StringBuilder();
    sb.append("TOTAL_TIME: " + totalTime +"\n"
        + "NUM_SPOUT: " + numSpouts + "\n" 
        + "TIMESCALES: " + timescales + "\n"
        + "START_TIME: " + System.currentTimeMillis() + "\n" 
        + "CACHING_PROB: " + cachingProb + "\n"
        + "NUM_TIMESCALES: " + timescales.size() + "\n"
        + "INPUT_TYPE: " + inputType + "\n"
        + "OPERATOR: " + operatorName);
    return sb.toString();
  }
}
