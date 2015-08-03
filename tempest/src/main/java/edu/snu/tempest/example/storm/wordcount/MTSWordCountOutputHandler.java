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

import edu.snu.tempest.example.util.writer.OutputWriter;
import edu.snu.tempest.operator.window.time.mts.MTSWindowOperator;
import edu.snu.tempest.operator.window.time.mts.MTSWindowOutput;

import java.io.IOException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Output handler for word count example.
 */
final class MTSWordCountOutputHandler implements MTSWindowOperator.MTSOutputHandler<Map<String, Long>> {
  private static final Logger LOG = Logger.getLogger(MTSWordCountOutputHandler.class.getName());

  /**
   * Output writer for logging.
   */
  private final OutputWriter writer;
  /**
   * Logging path.
   */
  private final String pathPrefix;

  /**
   * Output handler for word count example.
   * @param writer a writer
   * @param pathPrefix a logging path
   */
  public MTSWordCountOutputHandler(final OutputWriter writer,
                                    final String pathPrefix) {
    this.writer = writer;
    this.pathPrefix = pathPrefix;
  }

  /**
   * Logging the mts output.
   * @param output an mts output
   */
  @Override
  public void onNext(final MTSWindowOutput<Map<String, Long>> output) {
    long count = 0;
    // calculate total count for logging
    for (final Map.Entry<String, Long> entry : output.output.entrySet()) {
      count += entry.getValue();
    }

    try {
      writer.writeLine(pathPrefix + "/" + output.timescale.windowSize
          + "-" + output.timescale.intervalSize, (System.currentTimeMillis()) + "\t"
          + count);
    } catch (final IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    LOG.log(Level.INFO, "output of ts" + output.timescale + ": "
        + output.startTime + "-" + output.endTime + ", count: " + count);
  }
}