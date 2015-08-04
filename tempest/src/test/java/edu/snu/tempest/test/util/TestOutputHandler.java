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
package edu.snu.tempest.test.util;


import edu.snu.tempest.operator.window.time.TimeWindowOutput;
import edu.snu.tempest.operator.window.time.TimeWindowOutputHandler;
import edu.snu.tempest.operator.window.time.Timescale;

import javax.inject.Inject;
import java.util.Map;
import java.util.Queue;

/**
 * An output handler for test.
 */
public final class TestOutputHandler implements TimeWindowOutputHandler<Map<Integer, Long>> {
  private final Map<Timescale,
      Queue<TimeWindowOutput<Map<Integer, Long>>>> results;
  private final Monitor monitor;
  private int count = 0;

  @Inject
  public TestOutputHandler(final Monitor monitor,
                           final Map<Timescale, Queue<TimeWindowOutput<Map<Integer, Long>>>> results) {
    this.monitor = monitor;
    this.results = results;
  }

  /**
   * Collect window outputs.
   * @param windowOutput a mts window output
   */
  @Override
  public void onNext(final TimeWindowOutput<Map<Integer, Long>> windowOutput) {
    if (count < 2) {
      if (windowOutput.fullyProcessed) {
        final Queue<TimeWindowOutput<Map<Integer, Long>>> outputs = this.results.get(windowOutput.timescale);
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