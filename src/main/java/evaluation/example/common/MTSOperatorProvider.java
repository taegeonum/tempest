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

import edu.snu.tempest.operator.window.aggregator.CAAggregator;
import edu.snu.tempest.operator.window.timescale.*;
import edu.snu.tempest.operator.window.timescale.impl.TimescaleParser;
import org.apache.reef.tang.*;
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

  public MTSOperatorProvider(final List<Timescale> timescales,
                             final String operatorType,
                             final double cachingProb,
                             final Class<? extends CAAggregator> aggregator,
                             final Configuration conf,
                             final long startTime) {
    // create MTS operator
    final Configuration operatorConf;
    if (operatorType.equals("dynamic_random")) {
      operatorConf = DynamicMTSWindowConfiguration.CONF
          .set(DynamicMTSWindowConfiguration.START_TIME, startTime)
          .set(DynamicMTSWindowConfiguration.INITIAL_TIMESCALES, TimescaleParser.parseToString(timescales))
          .set(DynamicMTSWindowConfiguration.CA_AGGREGATOR, aggregator)
          .set(DynamicMTSWindowConfiguration.CACHING_PROB, cachingProb)
          .build();
    } else if (operatorType.equals("dynamic_dg")) {
      operatorConf = DynamicDGWindowConfiguration.CONF
          .set(DynamicMTSWindowConfiguration.START_TIME, startTime)
          .set(DynamicMTSWindowConfiguration.INITIAL_TIMESCALES, TimescaleParser.parseToString(timescales))
          .set(DynamicMTSWindowConfiguration.CA_AGGREGATOR, aggregator)
          .build();
    } else if (operatorType.equals("naive")) {
      operatorConf = NaiveMTSWindowConfiguration.CONF
          .set(NaiveMTSWindowConfiguration.CA_AGGREGATOR, aggregator)
          .set(NaiveMTSWindowConfiguration.INITIAL_TIMESCALES, TimescaleParser.parseToString(timescales))
          .set(NaiveMTSWindowConfiguration.START_TIME, startTime)
          .build();
    } else if (operatorType.equals("static_mts")) {
      operatorConf = StaticMTSWindowConfiguration.CONF
          .set(StaticMTSWindowConfiguration.CA_AGGREGATOR, aggregator)
          .set(StaticMTSWindowConfiguration.INITIAL_TIMESCALES, TimescaleParser.parseToString(timescales))
          .set(StaticMTSWindowConfiguration.START_TIME, startTime)
          .build();
    } else {
      throw new RuntimeException("Operator exception: " + operator);
    }

    final Injector ij = Tang.Factory.getTang().newInjector(Configurations.merge(operatorConf, conf));
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
