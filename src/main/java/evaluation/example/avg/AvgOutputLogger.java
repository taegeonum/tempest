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
package evaluation.example.avg;

import edu.snu.tempest.example.util.writer.OutputWriter;
import edu.snu.tempest.operator.OutputEmitter;
import edu.snu.tempest.operator.window.aggregator.impl.SumAndCount;
import edu.snu.tempest.operator.window.timescale.TimeWindowOutputHandler;
import edu.snu.tempest.operator.window.timescale.TimescaleWindowOutput;
import evaluation.example.common.OutputLogger;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.IOException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Output handler for word count example.
 */
public final class AvgOutputLogger implements TimeWindowOutputHandler<Map<String, SumAndCount>, Map<String, Long>> {
  private static final Logger LOG = Logger.getLogger(AvgOutputLogger.class.getName());

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
  @Inject
  private AvgOutputLogger(final OutputWriter writer,
                          @Parameter(OutputLogger.PathPrefix.class) final String pathPrefix) {
    this.writer = writer;
    this.pathPrefix = pathPrefix;
  }

  /**
   * Logging the time window output.
   * @param output an output
   */
  @Override
  public void execute(final TimescaleWindowOutput<Map<String, SumAndCount>> output) {
    long count = 0;
    try {
      // calculate total count for logging
      for (final Map.Entry<String, SumAndCount> entry : output.output.result.entrySet()) {
        count += entry.getValue().count;
      }

      try {
        writer.writeLine(pathPrefix + "/" + output.timescale.windowSize
            + "-" + output.timescale.intervalSize, (System.currentTimeMillis()) + "\t"
            + count + "\t" + output.output.numDepOutputs);
      } catch (final IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
      LOG.log(Level.INFO, "output of ts" + output.timescale + ": "
          + output.startTime + "-" + output.endTime + ", count: " + count);

    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public void prepare(final OutputEmitter<TimescaleWindowOutput<Map<String, Long>>> outputEmitter) {
    // end of operation
  }
}