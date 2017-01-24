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
package evaluation.example.topk;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import edu.snu.tempest.example.util.writer.LocalOutputWriter;
import edu.snu.tempest.example.util.writer.OutputWriter;
import edu.snu.tempest.operator.OperatorConnector;
import edu.snu.tempest.operator.OperatorDispatcher;
import edu.snu.tempest.operator.window.aggregator.impl.CountByKeyAggregator;
import edu.snu.tempest.operator.window.aggregator.impl.KeyExtractor;
import edu.snu.tempest.operator.window.timescale.DynamicMTSWindowOperator;
import edu.snu.tempest.operator.window.timescale.TimeWindowOutputHandler;
import edu.snu.tempest.operator.window.timescale.Timescale;
import atc.operator.window.timescale.parameter.NumThreads;
import evaluation.example.common.MTSOperatorProvider;
import evaluation.example.common.OutputLogger;
import evaluation.example.common.ResourceLogger;
import evaluation.example.common.StringKeyExtractor;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * MTSWordCountTestBolt.
 * It creates MTS operator and aggregates word and calculates counts.
 */
final class TopkDynamicBolt extends BaseRichBolt {
  /**
   * WordCount bolt using MTSOperator.
   */
  private static final long serialVersionUID = 1298716785871980572L;
  private static final Logger LOG = Logger.getLogger(TopkDynamicBolt.class.getName());

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
   * Caching prob for dynamic mts operator.
   */
  private final double cachingProb;

  /**
   * Window Operator.
   */
  private DynamicMTSWindowOperator<Tuple, Map<String, Long>> operator;

  private final List<Timescale> addTimescales;

  private final int tsAddInterval;

  private ExecutorService tsAddExecutor;

  /**
   * Start time.
   */
  private final long startTime;

  private ResourceLogger resourceLogger;
  private final int numThreads;

  /**
   * MTSWordCountTestBolt.
   * It aggregates word and calculates counts.
   * @param pathPrefix a path of log directory.
   * @param timescales an initial timescales.
   * @param operatorType a type of operator
   * @param cachingProb a caching prob for dynamic MTS operator.
   * @param startTime an initial start time.
   */
  public TopkDynamicBolt(final String pathPrefix,
                         final List<Timescale> timescales,
                         final List<Timescale> addTimescales,
                         final int tsAddInterval,
                         final String operatorType,
                         final double cachingProb,
                         final int numThreads,
                         final long startTime) {
    this.pathPrefix = pathPrefix;
    this.timescales = timescales;
    this.operatorType = operatorType;
    this.cachingProb = cachingProb;
    this.startTime = startTime;
    this.numThreads = numThreads;
    this.addTimescales = addTimescales;
    this.tsAddInterval = tsAddInterval;
  }
  
  @Override
  public void declareOutputFields(final OutputFieldsDeclarer paramOutputFieldsDeclarer) {
  }

  @Override
  public void execute(final Tuple tuple) {
    // send data to MTS operator.
    resourceLogger.execute(tuple);
    operator.execute(tuple);
  }

  @Override
  public void prepare(final Map conf, final TopologyContext paramTopologyContext,
                      final OutputCollector paramOutputCollector) {
    // logging resource
    resourceLogger = new ResourceLogger(pathPrefix);
    tsAddExecutor = Executors.newFixedThreadPool(1);

    // create MTS operator
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindImplementation(KeyExtractor.class, StringKeyExtractor.class);
    jcb.bindNamedParameter(NumThreads.class, numThreads + "");
    final MTSOperatorProvider<Tuple, Map<String, Long>> operatorProvider =
        new MTSOperatorProvider<>(timescales, operatorType, cachingProb,
            CountByKeyAggregator.class, jcb.build(), startTime);
    operator = (DynamicMTSWindowOperator)operatorProvider.getMTSOperator();
    final DynamicMTSWindowOperator<Tuple, Map<String, Long>> tsOperator = operator;
    try {
      final OutputWriter writer = Tang.Factory.getTang().newInjector().getInstance(LocalOutputWriter.class);
      writer.writeLine(pathPrefix + "/addTimescales", addTimescales.toString() + "\t" + "interval: " + tsAddInterval);
      tsAddExecutor.execute(new Runnable() {
        @Override
        public void run() {
          for (final Timescale addTs : addTimescales) {
            try {
              Thread.sleep(TimeUnit.SECONDS.toMillis(tsAddInterval));
              final long time = System.currentTimeMillis();
              final long nanoTime = System.nanoTime();
              tsOperator.onTimescaleAddition(addTs, TimeUnit.NANOSECONDS.toSeconds(nanoTime));
              final long endTime = System.currentTimeMillis();
              final long elapsed = endTime - time;
              writer.writeLine(pathPrefix + "/tsAddTime", time + "\t" + elapsed + "\t" + addTs);
            } catch (InterruptedException e) {
              e.printStackTrace();
            } catch (IOException e) {
              e.printStackTrace();
              throw new RuntimeException(e);
            }
          }
        }
      });
    } catch (InjectionException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    final Injector ij = Tang.Factory.getTang().newInjector();
    final OutputWriter writer;
    try {
      writer = ij.getInstance(LocalOutputWriter.class);
    } catch (InjectionException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    ij.bindVolatileInstance(OutputWriter.class, writer);
    ij.bindVolatileParameter(OutputLogger.PathPrefix.class, pathPrefix);
    try {
      final TimeWindowOutputHandler<Map<String, Long>, TopkOutput>
          topKOperator = ij.getInstance(TopKOperator.class);
      operator.prepare(new OperatorDispatcher<>(topKOperator, numThreads));
      final TimeWindowOutputHandler<TopkOutput, List<Map.Entry<String, Long>>>
          outputLogger = ij.getInstance(TopKOutputLogger.class);
      topKOperator.prepare(new OperatorConnector<>(outputLogger));
    } catch (InjectionException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public void cleanup() {
    if (resourceLogger != null) {
      resourceLogger.close();
    }
    try {
      operator.close();
      tsAddExecutor.shutdown();
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
}
