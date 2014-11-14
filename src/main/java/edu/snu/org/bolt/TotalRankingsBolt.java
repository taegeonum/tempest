/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.org.bolt;

import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import edu.snu.org.util.ValueAndTimestamp;

/**
 * Aggregates all counts and sorts by values
 */
public class TotalRankingsBolt extends BaseBasicBolt {

  private final int numOfInputBolts;
  private int count;
  private final int topN;
  private SortedSet<WordcountTuple> results;
  private final String name;
  private FileWriter writer;
  private long avgStartTime;
  private long totalCnt;
  private int startTimeCnt;
  
  public TotalRankingsBolt(final int topN, final int numOfInputBolts, String name) {
    this.numOfInputBolts = numOfInputBolts;
    this.topN = topN;
    this.name = name;
    count = 0;
    avgStartTime = 0;
    totalCnt = 0;
    results = new TreeSet<>();
  }
  
  @Override
  public void prepare(Map stormConf, TopologyContext context) {
    try {
      writer = new FileWriter(name);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    count++;
    Map<String, ValueAndTimestamp<Integer>> aggWordCnt = (Map) tuple.getValue(0);
    long avgSt = (long)tuple.getValue(1);
    avgStartTime += avgSt;
    
    if (avgSt != 0) {
      startTimeCnt++;
    }
    
    totalCnt += (long)tuple.getValue(2);
    
    // sort
    for (Map.Entry<String, ValueAndTimestamp<Integer>> entry : aggWordCnt.entrySet()) {
      WordcountTuple wcTuple = new WordcountTuple(entry.getKey(), entry.getValue().getValue());
      results.add(wcTuple);
    }

    if (count == numOfInputBolts) {
      // flush 
      count = 0;
      
      List<WordcountTuple> list = new LinkedList<>();
      int i = 0;
      for ( WordcountTuple tup : results) {
        if (i >= topN) {
          break;
        }
        
        list.add(tup);
      }
      //collector.emit(new Values(list));
      long endTime = System.currentTimeMillis();
      long latency = endTime - (avgStartTime/startTimeCnt);
      
      try {
        writer.write(latency + "\t" + totalCnt + "\n");
      } catch (IOException e) {
        e.printStackTrace();
      }
      results.clear();
      
      avgStartTime = totalCnt = startTimeCnt = 0;
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("rankings"));
  }
  
  @Override
  public void cleanup() {
    try {
      writer.close();
      // TODO: copy local fail to HDFS 
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  public class WordcountTuple implements Comparable<WordcountTuple> {
    private final String key;
    private final int count;
    
    public WordcountTuple(String key, int count) {
      this.key = key;
      this.count = count;
    }

    @Override
    public int compareTo(WordcountTuple o) {      
      if (count < o.count) {
        return 1;
      } else if (count > o.count){
        return -1;
      } else {
        return 0;
      }
    }
    
    @Override
    public String toString() {
      return "(K: " + key + ", V: " + count + ")";
    }
  }
}