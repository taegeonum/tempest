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
package edu.snu.tempest.example.storm.wordcount;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.apache.commons.math3.random.RandomDataGenerator;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Sends randomly selected words which have zipfian distribution continuously.
 */
public final class ZipfianWordSpout extends BaseRichSpout {
  private static final Logger LOG = Logger.getLogger(ZipfianWordSpout.class.getName());
  private static final int NUM_OF_WORDS = 15 * 15 * 15 * 15;
  private static final double ZIPF_CONSTANT = 1.4;

  SpoutOutputCollector collector;
  private final RandomDataGenerator random = new RandomDataGenerator();
  private final int sendingInterval;
  
  public ZipfianWordSpout(final double sendingInterval) {
    this.sendingInterval = (int) sendingInterval;
  }

  @Override
  public void open(final Map conf,
                   final TopologyContext context,
                   final SpoutOutputCollector col) {
    LOG.log(Level.INFO, ZipfianWordSpout.class.getName() + " started");
    this.collector = col;
  }

  @Override
  public void nextTuple() {
    Utils.sleep(sendingInterval);
    for (int i = 0; i < 5; i++) {
      this.collector.emit(new Values(Integer.toString(random.nextZipf(NUM_OF_WORDS, ZIPF_CONSTANT)),
          1, System.currentTimeMillis()));
    }
  }

  @Override
  public void ack(final Object id) {
  }

  @Override
  public void fail(final Object id) {
    throw new RuntimeException();
  }

  @Override
  public void declareOutputFields(final OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("word", "count", "timestamp"));
  }
}
