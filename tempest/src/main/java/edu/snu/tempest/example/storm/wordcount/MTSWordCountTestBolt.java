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

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import edu.snu.tempest.example.util.writer.LocalOutputWriter;
import edu.snu.tempest.example.util.writer.OutputWriter;
import edu.snu.tempest.operator.window.WindowOperator;
import edu.snu.tempest.operator.window.aggregator.CAAggregator;
import edu.snu.tempest.operator.window.aggregator.impl.CountByKeyAggregator;
import edu.snu.tempest.operator.window.aggregator.impl.KeyExtractor;
import edu.snu.tempest.operator.window.time.*;
import edu.snu.tempest.operator.window.time.parameter.CachingRate;
import edu.snu.tempest.operator.window.time.parameter.StartTime;
import edu.snu.tempest.util.Profiler;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.impl.StageManager;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

/**
 * MTSWordCountTestBolt.
 * It creates MTS operator and aggregates word and calculates counts.
 */
final class MTSWordCountTestBolt extends BaseRichBolt {
  /**
   * WordCount bolt using MTSOperator.
   */
  private static final long serialVersionUID = 1298716785871980572L;
  private static final Logger LOG = Logger.getLogger(MTSWordCountTestBolt.class.getName());

  /**
   * OutputWriter for logging.
   */
  private OutputWriter writer;

  /**
   * Log directory.
   */
  private final String pathPrefix;

  /**
   * Timescales.
   */
  private final List<Timescale> timescales;

  /**
   * A type of operator.
   * [dynamic_mts, naive, static_mts]
   */
  private final String operatorType;

  /**
   * Caching rate for dynamic mts operator.
   */
  private final double cachingRate;

  /**
   * Zookeeper address.
   */
  private final String address;

  /**
   * Window Operator.
   */
  private WindowOperator<Tuple> operator;

  /**
   * Scheduled executor for logging.
   */
  private ScheduledExecutorService executor;

  /**
   * Number of execution.
   */
  private final AtomicLong numOfExecution = new AtomicLong();

  /**
   * Start time.
   */
  private final long startTime;

  /**
   * MTSWordCountTestBolt.
   * It aggregateds word and calculates counts.
   * @param pathPrefix a path of log directory.
   * @param timescales an initial timescales.
   * @param operatorType a type of operator
   * @param address a zookeeper address for receiving timescale addition/deletion.
   * @param cachingRate a caching rate for dynamic MTS operator.
   * @param startTime an initial start time.
   */
  public MTSWordCountTestBolt(final String pathPrefix,
                              final List<Timescale> timescales,
                              final String operatorType,
                              final String address,
                              final double cachingRate,
                              final long startTime) {
    this.pathPrefix = pathPrefix;
    this.timescales = timescales;
    this.operatorType = operatorType;
    this.address = address;
    this.cachingRate = cachingRate;
    this.startTime = startTime;
  }
  
  @Override
  public void declareOutputFields(final OutputFieldsDeclarer paramOutputFieldsDeclarer) {
  }

  @Override
  public void execute(final Tuple tuple) {
    // send data to MTS operator.
    operator.execute(tuple);
    numOfExecution.incrementAndGet();
  }

  @Override
  public void prepare(final Map conf, final TopologyContext paramTopologyContext,
                      final OutputCollector paramOutputCollector) {
    try {
      this.writer = Tang.Factory.getTang().newInjector().getInstance(LocalOutputWriter.class);
    } catch (final InjectionException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

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

    // create MTS operator
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.bindImplementation(CAAggregator.class, CountByKeyAggregator.class);
    cb.bindNamedParameter(CachingRate.class, cachingRate + "");
    cb.bindNamedParameter(StartTime.class, this.startTime + "");

    final Configuration operatorConf;
    if (operatorType.equals("dynamic_mts")) {
      operatorConf = DynamicMTSWindowConfiguration.CONF
          .set(DynamicMTSWindowConfiguration.START_TIME, startTime)
          .set(DynamicMTSWindowConfiguration.INITIAL_TIMESCALES, TimescaleParser.parseToString(timescales))
          .set(DynamicMTSWindowConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
          .set(DynamicMTSWindowConfiguration.OUTPUT_HANDLER, WordCountOutputHandler.class)
          .set(DynamicMTSWindowConfiguration.CACHING_RATE, cachingRate)
          .set(DynamicMTSWindowConfiguration.OPERATOR_IDENTIFIER, "mts-wcbolt")
          .set(DynamicMTSWindowConfiguration.ZK_SERVER_ADDRESS, address)
          .build();
    } else if (operatorType.equals("naive")) {
      final int index = paramTopologyContext.getThisTaskIndex();
      final List<Timescale> tsList = new LinkedList<>();
      tsList.add(timescales.get(index));
      operatorConf = StaticMTSWindowConfiguration.CONF
          .set(StaticMTSWindowConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
          .set(StaticMTSWindowConfiguration.INITIAL_TIMESCALES, TimescaleParser.parseToString(tsList))
          .set(StaticMTSWindowConfiguration.OUTPUT_HANDLER, WordCountOutputHandler.class)
          .set(StaticMTSWindowConfiguration.START_TIME, startTime)
          .build();
    } else if (operatorType.equals("static_mts")) {
      operatorConf = StaticMTSWindowConfiguration.CONF
          .set(StaticMTSWindowConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
          .set(StaticMTSWindowConfiguration.INITIAL_TIMESCALES, TimescaleParser.parseToString(timescales))
          .set(StaticMTSWindowConfiguration.OUTPUT_HANDLER, WordCountOutputHandler.class)
          .set(StaticMTSWindowConfiguration.START_TIME, startTime)
          .build();
    } else {
      throw new RuntimeException("Operator exception: " + operator);
    }

    final Injector ij = Tang.Factory.getTang().newInjector(operatorConf);
    ij.bindVolatileInstance(KeyExtractor.class,
        new KeyExtractor<Tuple, String>() {
          @Override
          public String getKey(final Tuple tuple) {
            return tuple.getString(0);
          }
        });
    ij.bindVolatileInstance(OutputWriter.class, writer);
    ij.bindVolatileParameter(WordCountOutputHandler.PathPrefix.class, pathPrefix);
    try {
      operator = ij.getInstance(WindowOperator.class);
    } catch (InjectionException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public void cleanup() {
    try {
      this.executor.shutdown();
      StageManager.instance().close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
