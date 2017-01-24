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
package atc.evaluation.common;

import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import atc.operator.window.aggregator.CAAggregator;
import atc.operator.window.timescale.Timescale;
import atc.operator.window.timescale.TimescaleWindowOperator;
import atc.operator.window.timescale.common.TimescaleParser;
import atc.operator.window.timescale.pafas.StaticMWOConfiguration;

import java.util.List;
import java.util.logging.Logger;

/**
 * MTSWordCountTestBolt.
 * It creates MTS operator and aggregates word and calculates counts.
 */
public final class MWOProvider<I, V> {
  /**
   * WordCount bolt using MTSOperator.
   */
  private static final long serialVersionUID = 1298716785871980572L;
  private static final Logger LOG = Logger.getLogger(MWOProvider.class.getName());

  /**
   * Window Operator.
   */
  private TimescaleWindowOperator<I, V> operator;

  public MWOProvider(final List<Timescale> timescales,
                     final String operatorType,
                     final double cachingProb,
                     final Class<? extends CAAggregator> aggregator,
                     final Configuration conf,
                     final long startTime) {
    // create MTS operator
    final Configuration operatorConf;
    if (operatorType.equals("static_pafas")) {
      operatorConf = StaticMWOConfiguration.CONF
          //.set(StaticMWOConfiguration.START_TIME, startTime)
          .set(StaticMWOConfiguration.INITIAL_TIMESCALES, TimescaleParser.parseToString(timescales))
          .set(StaticMWOConfiguration.CA_AGGREGATOR, aggregator)
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
