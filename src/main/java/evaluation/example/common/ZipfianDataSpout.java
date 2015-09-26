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
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Map;
import java.util.Scanner;

public final class ZipfianDataSpout extends BaseRichSpout {


  @NamedParameter(short_name = "input_path")
  public final static class InputPath implements Name<String> {}

  private final String inputPath;

  private Scanner sc;

  private SpoutOutputCollector collector;

  private File inputFile;

  private final long inputRate;

  public ZipfianDataSpout(
                          final String inputPath,
                          final long inputRate) {
    this.inputPath = inputPath;
    this.inputRate = inputRate;
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
    try {
      Thread.sleep(1);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    final int loop = (int)inputRate / 1000;

    for (int i = 0; i < loop; i++){
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
