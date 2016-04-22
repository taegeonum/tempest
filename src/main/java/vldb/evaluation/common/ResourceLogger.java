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
package vldb.evaluation.common;

import backtype.storm.tuple.Tuple;
import edu.snu.tempest.example.util.writer.LocalOutputWriter;
import edu.snu.tempest.example.util.writer.OutputWriter;
import edu.snu.tempest.util.Profiler;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;


public final class ResourceLogger implements AutoCloseable {

  private static final Logger LOG = Logger.getLogger(ResourceLogger.class.getName());

  /**
   * Scheduled executor for logging.
   */
  private ScheduledExecutorService executor;

  /**
   * Number of execution.
   */
  private final AtomicLong numOfExecution = new AtomicLong();

  public ResourceLogger(final String pathPrefix) {
    try {
      final OutputWriter writer = Tang.Factory.getTang().newInjector().getInstance(LocalOutputWriter.class);
      // profiling
      this.executor = Executors.newScheduledThreadPool(3);
      this.executor.scheduleAtFixedRate(new Runnable() {
        @Override
        public void run() {
          try {
            // cpu logging
            writer.writeLine(pathPrefix + "/cpu", (System.currentTimeMillis()) + "\t"
                + Profiler.getCpuLoad());
            // memory logging
            writer.writeLine(pathPrefix + "/memory", (System.currentTimeMillis()) + "\t"
                + Profiler.getMemoryUsage());
            final long executionNum = numOfExecution.get();
            // number of execution
            writer.writeLine(pathPrefix + "/slicedWindowExecution", (System.currentTimeMillis()) + "\t"
                + executionNum);
            numOfExecution.addAndGet(-1 * executionNum);
          } catch (final IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
          }
        }
      }, 0, 1, TimeUnit.SECONDS);
    } catch (final InjectionException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public void execute(final Tuple tuple) {
    // send data to MTS operator.
    numOfExecution.incrementAndGet();
  }

  @Override
  public void close() {
    try {
      this.executor.shutdown();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
