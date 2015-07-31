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
package edu.snu.tempest.util;


import edu.snu.tempest.operator.window.Timescale;
import edu.snu.tempest.operator.window.mts.MTSWindowOperator;
import edu.snu.tempest.operator.window.mts.MTSWindowOutput;

import java.util.Map;
import java.util.Queue;

public final class TestOutputHandler implements MTSWindowOperator.MTSOutputHandler<Map<Integer, Long>> {
  private final Map<Timescale,
      Queue<MTSWindowOutput<Map<Integer, Long>>>> results;
  private final long startTime;
  private final Monitor monitor;
  private int count = 0;

  public TestOutputHandler(final Monitor monitor,
                           final Map<Timescale, Queue<MTSWindowOutput<Map<Integer, Long>>>> results,
                           final long startTime) {
    this.monitor = monitor;
    this.results = results;
    this.startTime = startTime;
  }

  @Override
  public void onNext(final MTSWindowOutput<Map<Integer, Long>> windowOutput) {
    if (count < 2) {
      if (windowOutput.fullyProcessed) {
        Queue<MTSWindowOutput<Map<Integer, Long>>> outputs = this.results.get(windowOutput.timescale);
        System.out.println(windowOutput);
        outputs.add(windowOutput);
      }
    } else {
      this.monitor.mnotify();
    }

    if (windowOutput.timescale.windowSize == 8) {
      count++;
    }
  }
}