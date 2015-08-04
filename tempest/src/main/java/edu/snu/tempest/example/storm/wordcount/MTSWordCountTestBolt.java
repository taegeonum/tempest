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
import edu.snu.tempest.example.util.writer.OutputWriter;
import edu.snu.tempest.operator.window.WindowOperator;
import edu.snu.tempest.operator.window.aggregator.CAAggregator;
import edu.snu.tempest.operator.window.aggregator.impl.CountByKeyAggregator;
import edu.snu.tempest.operator.window.aggregator.impl.KeyExtractor;
import edu.snu.tempest.operator.window.time.Timescale;
import edu.snu.tempest.operator.window.time.mts.MTSWindowOperator;
import edu.snu.tempest.operator.window.time.mts.impl.DynamicMTSOperatorImpl;
import edu.snu.tempest.operator.window.time.mts.impl.OTFMTSOperatorImpl;
import edu.snu.tempest.operator.window.time.mts.impl.StaticMTSOperatorImpl;
import edu.snu.tempest.operator.window.time.mts.parameters.CachingRate;
import edu.snu.tempest.operator.window.time.mts.parameters.StartTime;
import edu.snu.tempest.operator.window.time.mts.signal.MTSSignalReceiver;
import edu.snu.tempest.operator.window.time.mts.signal.impl.ZkMTSParameters;
import edu.snu.tempest.operator.window.time.mts.signal.impl.ZkSignalReceiver;
import edu.snu.tempest.operator.window.time.sts.STSWindowOperator;
import edu.snu.tempest.operator.window.time.sts.impl.STSWindowOperatorImpl;
import edu.snu.tempest.util.Profiler;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
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
  private final OutputWriter writer;

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
   * [dynamic_mts, naive, static_mts, otf]
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
  private final long startTime;
  private long totalBytes = 0;
  private long sizeOfInt = 4;
  private long sizeOfLong = 8;

  /**
   * MTSWordCountTestBolt.
   * It aggregateds word and calculates counts.
   * @param writer a writer for logging.
   * @param pathPrefix a path of log directory.
   * @param timescales an initial timescales.
   * @param operatorType a type of operator
   * @param address a zookeeper address for receiving timescale addition/deletion.
   * @param cachingRate a caching rate for dynamic MTS operator.
   * @param startTime an initial start time.
   */
  public MTSWordCountTestBolt(final OutputWriter writer,
                              final String pathPrefix,
                              final List<Timescale> timescales,
                              final String operatorType,
                              final String address,
                              final double cachingRate,
                              final long startTime) {
    this.writer = writer;
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
    totalBytes += (tuple.getString(0).length() + sizeOfInt + sizeOfLong);
    // send data to MTS operator.
    operator.execute(tuple);
    numOfExecution.incrementAndGet();
  }

  @Override
  public void prepare(final Map conf, final TopologyContext paramTopologyContext,
                      final OutputCollector paramOutputCollector) {
    // profiling 
    this.executor = Executors.newScheduledThreadPool(3);
    this.executor.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
          try {
            // cpu logging
            writer.writeLine(pathPrefix + "/cpu", (System.currentTimeMillis()) + "\t" 
                + Profiler.getCpuLoad());
            // bytes logging
            writer.writeLine(pathPrefix + "/bytes", (System.currentTimeMillis()) + "\t" 
                + totalBytes);
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
          LOG.log(Level.INFO, (System.currentTimeMillis()) + "\t" 
              + totalBytes);
      }
    }, 0, 1, TimeUnit.SECONDS);

    // create MTS operator
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.bindImplementation(CAAggregator.class, CountByKeyAggregator.class);
    cb.bindNamedParameter(CachingRate.class, cachingRate + "");
    cb.bindNamedParameter(StartTime.class, this.startTime + "");

    if (operatorType.equals("dynamic_mts")) {
      cb.bindImplementation(MTSWindowOperator.class, DynamicMTSOperatorImpl.class);
      cb.bindNamedParameter(ZkMTSParameters.OperatorIdentifier.class, "mts-wcbolt");
      cb.bindNamedParameter(ZkMTSParameters.ZkServerAddress.class, address);
      cb.bindImplementation(MTSSignalReceiver.class, ZkSignalReceiver.class);
    } else if (operatorType.equals("naive")) {
      cb.bindImplementation(STSWindowOperator.class, STSWindowOperatorImpl.class);
    } else if (operatorType.equals("static_mts")) {
      cb.bindImplementation(MTSWindowOperator.class, StaticMTSOperatorImpl.class);
    } else if (operatorType.equals("otf")) {
      cb.bindImplementation(MTSWindowOperator.class, OTFMTSOperatorImpl.class);
    } else {
      throw new RuntimeException("Operator exception: " + operator);
    }

    final Injector ij = Tang.Factory.getTang().newInjector(cb.build());
    ij.bindVolatileInstance(KeyExtractor.class,
        new KeyExtractor<Tuple, String>() {
          @Override
          public String getKey(final Tuple tuple) {
            return tuple.getString(0);
          }
        });

    if (operatorType.equals("naive")) {
      // bind one timescale to each executors.
      final int index = paramTopologyContext.getThisTaskIndex();
      ij.bindVolatileInstance(Timescale.class, timescales.get(index));
      ij.bindVolatileInstance(STSWindowOperator.STSOutputHandler.class,
          new STSWordCountOutputHandler(writer, pathPrefix, timescales.get(index)));
      try {
        operator = ij.getInstance(STSWindowOperator.class);
      } catch (final InjectionException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    } else {
      ij.bindVolatileInstance(List.class, timescales);
      ij.bindVolatileInstance(MTSWindowOperator.MTSOutputHandler.class,
          new MTSWordCountOutputHandler(writer, pathPrefix));
      try {
        operator = ij.getInstance(MTSWindowOperator.class);
      } catch (final InjectionException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }
    operator.start();
  }

  @Override
  public void cleanup() {
    try {
      this.operator.close();
      this.executor.shutdown();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
