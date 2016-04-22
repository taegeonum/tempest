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
import edu.snu.tempest.operator.window.timescale.TimeWindowOutputHandler;
import edu.snu.tempest.operator.window.timescale.Timescale;
import edu.snu.tempest.operator.window.timescale.TimescaleWindowOperator;
import vldb.operator.window.timescale.parameter.NumThreads;
import evaluation.example.common.MTSOperatorProvider;
import evaluation.example.common.OutputLogger;
import evaluation.example.common.ResourceLogger;
import evaluation.example.common.StringKeyExtractor;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;

import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * MTSWordCountTestBolt.
 * It creates MTS operator and aggregates word and calculates counts.
 */
final class TopkBolt extends BaseRichBolt {
  /**
   * WordCount bolt using MTSOperator.
   */
  private static final long serialVersionUID = 1298716785871980572L;
  private static final Logger LOG = Logger.getLogger(TopkBolt.class.getName());

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
  private TimescaleWindowOperator<Tuple, Map<String, Long>> operator;

  /**
   * Start time.
   */
  private final long startTime;

  private ResourceLogger resourceLogger;

  private final int numThreads;

  private OutputWriter writer;

  /**
   * MTSWordCountTestBolt.
   * It aggregates word and calculates counts.
   * @param pathPrefix a path of log directory.
   * @param timescales an initial timescales.
   * @param operatorType a type of operator
   * @param cachingProb a caching prob for dynamic MTS operator.
   * @param startTime an initial start time.
   */
  public TopkBolt(final String pathPrefix,
                  final List<Timescale> timescales,
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

    // create MTS operator
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindImplementation(KeyExtractor.class, StringKeyExtractor.class);
    jcb.bindNamedParameter(NumThreads.class, numThreads + "");
    final MTSOperatorProvider<Tuple, Map<String, Long>> operatorProvider =
        new MTSOperatorProvider<>(timescales, operatorType, cachingProb,
            CountByKeyAggregator.class, jcb.build(), startTime);
    operator = operatorProvider.getMTSOperator();

    final Injector ij = Tang.Factory.getTang().newInjector();
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
      writer.close();
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
}
