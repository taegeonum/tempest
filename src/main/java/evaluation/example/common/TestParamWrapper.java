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
package evaluation.example.common;

import edu.snu.tempest.example.storm.parameter.*;
import edu.snu.tempest.operator.window.timescale.Timescale;
import edu.snu.tempest.operator.window.timescale.impl.TimescaleParser;
import edu.snu.tempest.operator.window.timescale.parameter.CachingProb;
import edu.snu.tempest.operator.window.timescale.parameter.TimescaleString;
import evaluation.example.parameter.NumOfKey;
import evaluation.example.parameter.ZipfianConstant;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.List;

/**
 * Class for parsing parameters for test.
 */
public final class TestParamWrapper {
  public final int numSpouts;
  public final String testName;
  public final String logDir;
  public final double cachingProb;
  public final int totalTime;
  public final String operatorName;
  public final String inputType;
  public final List<Timescale> timescales;
  public final double inputInterval;
  public final double zipfConst;
  public final long numKey;

  @Inject
  private TestParamWrapper(@Parameter(NumSpouts.class) final int numSpouts,
                           @Parameter(TestName.class) final String testName,
                           @Parameter(LogDir.class) final String logDir,
                           @Parameter(CachingProb.class) final double cachingProb,
                           @Parameter(TotalTime.class) final int totalTime,
                           @Parameter(OperatorType.class) final String operator,
                           @Parameter(InputType.class) final String inputType,
                           @Parameter(InputInterval.class) final double inputInterval,
                           @Parameter(TimescaleString.class) final String timescaleParameter,
                           @Parameter(ZipfianConstant.class) final double zipfConst,
                           @Parameter(NumOfKey.class) final long numKey) {
    this.numSpouts = numSpouts;
    this.testName = testName;
    this.logDir = logDir;
    this.cachingProb = cachingProb;
    this.totalTime = totalTime;
    this.timescales = TimescaleParser.parseFromString(timescaleParameter);
    this.operatorName = operator;
    this.inputType = inputType;
    this.inputInterval = inputInterval;
    this.zipfConst = zipfConst;
    this.numKey = numKey;
  }
  
  public String print() {
    StringBuilder sb = new StringBuilder();
    sb.append("TOTAL_TIME: " + totalTime +"\n"
        + "NUM_SPOUT: " + numSpouts + "\n" 
        + "TIMESCALES: " + timescales + "\n"
        + "START_TIME: " + System.currentTimeMillis() + "\n" 
        + "CACHING_PROB: " + cachingProb + "\n"
        + "NUM_TIMESCALES: " + timescales.size() + "\n"
        + "NUM_KEY: " + numKey + "\n"
        + "ZIPF_CONST: " + zipfConst + "\n"
        + "INPUT_TYPE: " + inputType + "\n"
        + "INPUT_RATE: " + (1000.0 / inputInterval) * numSpouts + "\n"
        + "OPERATOR: " + operatorName);
    return sb.toString();
  }
}
