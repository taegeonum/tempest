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
package evaluation.example.common;

import backtype.storm.tuple.Tuple;
import edu.snu.tempest.operator.window.aggregator.CAAggregator;
import edu.snu.tempest.operator.window.aggregator.impl.CountByKeyAggregator;
import edu.snu.tempest.operator.window.aggregator.impl.KeyExtractor;
import edu.snu.tempest.operator.window.timescale.*;
import edu.snu.tempest.operator.window.timescale.impl.TimescaleParser;
import edu.snu.tempest.operator.window.timescale.parameter.CachingProb;
import edu.snu.tempest.operator.window.timescale.parameter.StartTime;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;

import java.util.List;
import java.util.logging.Logger;

/**
 * MTSWordCountTestBolt.
 * It creates MTS operator and aggregates word and calculates counts.
 */
public final class MTSOperatorProvider<I, V> {
  /**
   * WordCount bolt using MTSOperator.
   */
  private static final long serialVersionUID = 1298716785871980572L;
  private static final Logger LOG = Logger.getLogger(MTSOperatorProvider.class.getName());

  /**
   * Window Operator.
   */
  private TimescaleWindowOperator<I, V> operator;

  public MTSOperatorProvider(final String pathPrefix,
                              final List<Timescale> timescales,
                              final String operatorType,
                              final double cachingProb,
                              final long startTime) {
    // create MTS operator
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.bindImplementation(CAAggregator.class, CountByKeyAggregator.class);
    cb.bindNamedParameter(CachingProb.class, cachingProb + "");
    cb.bindNamedParameter(StartTime.class, startTime + "");

    final Configuration operatorConf;
    if (operatorType.equals("dynamic_mts")) {
      operatorConf = DynamicMTSWindowConfiguration.CONF
          .set(DynamicMTSWindowConfiguration.START_TIME, startTime)
          .set(DynamicMTSWindowConfiguration.INITIAL_TIMESCALES, TimescaleParser.parseToString(timescales))
          .set(DynamicMTSWindowConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
          .set(DynamicMTSWindowConfiguration.CACHING_PROB, cachingProb)
          .build();
    } else if (operatorType.equals("naive")) {
      operatorConf = NaiveMTSWindowConfiguration.CONF
          .set(NaiveMTSWindowConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
          .set(NaiveMTSWindowConfiguration.INITIAL_TIMESCALES, TimescaleParser.parseToString(timescales))
          .set(NaiveMTSWindowConfiguration.START_TIME, startTime)
          .build();
    } else if (operatorType.equals("static_mts")) {
      operatorConf = StaticMTSWindowConfiguration.CONF
          .set(StaticMTSWindowConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
          .set(StaticMTSWindowConfiguration.INITIAL_TIMESCALES, TimescaleParser.parseToString(timescales))
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
    try {
      operator = ij.getInstance(TimescaleWindowOperator.class);
    } catch (InjectionException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public TimescaleWindowOperator<I, V> getMTSOperator() {
    return operator;
  }
}
