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

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;

import java.util.Map;

public final class WikiDataSpout extends BaseRichSpout {

  private final double inputInterval;

  public WikiDataSpout(final double inputInterval) {
    this.inputInterval = inputInterval;
  }

  @Override
  public void declareOutputFields(final OutputFieldsDeclarer outputFieldsDeclarer) {

  }

  @Override
  public void open(final Map map,
                   final TopologyContext topologyContext,
                   final SpoutOutputCollector spoutOutputCollector) {

  }

  @Override
  public void nextTuple() {

  }
}
