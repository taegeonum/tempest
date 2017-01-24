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

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import edu.snu.tempest.operator.Operator;
import edu.snu.tempest.operator.OperatorConnector;
import edu.snu.tempest.operator.OutputEmitter;
import edu.snu.tempest.operator.filter.FilterFunction;
import edu.snu.tempest.operator.filter.FilterOperator;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.exceptions.InjectionException;

import java.util.Map;

public final class SplitFilterBolt extends BaseRichBolt {

  private Operator<String, String> splitOperator;

  @NamedParameter(short_name="num_split_bolt", default_value="20")
  public static final class NumSplitBolt implements Name<Integer> {}

  public SplitFilterBolt() {
  }
  
  @Override
  public void declareOutputFields(final OutputFieldsDeclarer paramOutputFieldsDeclarer) {
    paramOutputFieldsDeclarer.declare(new Fields("word"));
  }

  @Override
  public void execute(final Tuple tuple) {
    splitOperator.execute(tuple.getString(0));
  }

  @Override
  public void prepare(final Map conf, final TopologyContext paramTopologyContext,
                      final OutputCollector paramOutputCollector) {
    // create MTS operator
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindImplementation(FilterFunction.class, WikiFilterFunction.class);

    final Injector ij = Tang.Factory.getTang().newInjector(jcb.build());
    try {
      splitOperator = ij.getInstance(WikiSplitOperator.class);
      final Operator<String, String> filterOperator = ij.getInstance(FilterOperator.class);
      splitOperator.prepare(new OperatorConnector<>(filterOperator));
      filterOperator.prepare(new OutputEmitter<String>() {
        @Override
        public void emit(final String output) {
          paramOutputCollector.emit(new Values(output));
        }

        @Override
        public void close() throws Exception {
        }
      });
    } catch (InjectionException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public void cleanup() {
  }
}
