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
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Map;
import java.util.Scanner;

public final class WikiDataSpout extends BaseRichSpout {

  private final int inputInterval;

  @NamedParameter(short_name = "input_path")
  public final static class InputPath implements Name<String> {}

  private final String inputPath;

  private Scanner sc;

  private SpoutOutputCollector collector;

  private File inputFile;

  public WikiDataSpout(final double inputInterval,
                       final String inputPath) {
    this.inputInterval = (int)inputInterval;
    this.inputPath = inputPath;
  }

  @Override
  public void declareOutputFields(final OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declare(new Fields("word"));
  }

  @Override
  public void open(final Map map,
                   final TopologyContext topologyContext,
                   final SpoutOutputCollector spoutOutputCollector) {
    final int index = topologyContext.getThisTaskIndex();
    collector = spoutOutputCollector;
    inputFile = new File(inputPath + index);

    if (!inputFile.isFile()) {
      throw new RuntimeException(inputFile + " is not file");
    }

    try {
      sc = new Scanner(inputFile);
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void nextTuple() {
    Utils.sleep(inputInterval);
    if (sc.hasNextLine()) {
      collector.emit(new Values(sc.nextLine()));
    } else {
      sc.close();

      try {
        sc = new Scanner(inputFile);
      } catch (FileNotFoundException e) {
        throw new RuntimeException(e);
      }

      if (sc.hasNextLine()) {
        collector.emit(new Values(sc.nextLine()));
      }
    }
  }

  @Override
  public void close() {
    try {
      sc.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
