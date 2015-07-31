/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package edu.snu.tempest.example.storm.wordcount;

import edu.snu.tempest.example.storm.parameters.*;
import edu.snu.tempest.example.util.TimescaleParser;
import edu.snu.tempest.operator.window.mts.parameters.CachingRate;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 * Class for parsing parameters for test.
 */
public final class WordCountTestUtil {
  public final int numSpouts;
  public final String testName;
  public final String logDir;
  public final double cachingRate;
  public final int totalTime;
  public final TimescaleParser tsParser;
  public final String operatorName;
  public final String inputType;
  
  @Inject
  public WordCountTestUtil(@Parameter(NumSpouts.class) final int numSpouts,
                           @Parameter(TestName.class) final String testName,
                           @Parameter(LogDir.class) final String logDir,
                           @Parameter(CachingRate.class) final double cachingRate,
                           @Parameter(TotalTime.class) final int totalTime,
                           @Parameter(Operator.class) final String operator,
                           @Parameter(InputType.class) final String inputType,
                           final TimescaleParser tsParser) {
    this.numSpouts = numSpouts;
    this.testName = testName;
    this.logDir = logDir;
    this.cachingRate = cachingRate;
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
        + "SAVING_RATE: " + cachingRate + "\n"
        + "NUM_TIMESCALES: " + tsParser.timescales.size() + "\n"
        + "INPUT_TYPE: " + inputType + "\n"
        + "OPERATOR: " + operatorName);
    
    return sb.toString();
  }
}
