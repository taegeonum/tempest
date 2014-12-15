package edu.snu.org.grouping;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.impl.ThreadPoolStage;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import edu.snu.org.TestApp.NumBolt;
import edu.snu.org.util.OutputWriter;
import edu.snu.org.util.ValueAndTimestampWithCount;

public class WordGroupingMergeBolt extends BaseBasicBolt {

  private final OutputWriter writer;
  private final int numOfInputBolts;
  private ThreadPoolStage<String> outStage;
  private int count;
  private long avgStartTime;
  private long totalCnt;
  private int startTimeCnt;
  private final Map<Character, Long> results;

  
  @Inject
  public WordGroupingMergeBolt(
      @Parameter(NumBolt.class) final int numOfInputBolts, 
      OutputWriter writer) {
    
    this.numOfInputBolts = numOfInputBolts;
    this.writer = writer;
    this.results = new HashMap<>();
    
    count = 0;
    avgStartTime = 0;
    totalCnt = 0;
  }
  
  @Override
  public void prepare(Map stormConf, TopologyContext context) {
    
    outStage = new ThreadPoolStage<>("outThread", new EventHandler<String>() {

      @Override
      public void onNext(String paramT) {
        writer.writeLine(paramT);
      }

    }, 1);
  }

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    count++;
    Map<Character, ValueAndTimestampWithCount<Long>> histogram = (Map) tuple.getValue(0);
    long avgSt = (long)tuple.getLong(1);
    avgStartTime += avgSt;
    
    if (avgSt != 0) {
      startTimeCnt++;
    }
    
    totalCnt += (long)tuple.getLong(2);
    

    // merge
    for (Map.Entry<Character, ValueAndTimestampWithCount<Long>> entry : histogram.entrySet()) {
      Character key = entry.getKey();
      Long val = entry.getValue().value;
      Long histo = results.get(key);
      
      if (histo == null) {
        results.put(key, entry.getValue().value);
      } else {
        results.put(key, val + histo);
      }
    }

    if (count == numOfInputBolts) {
      System.out.println(results);
      // flush 
      count = 0;
      
      //collector.emit(new Values(list));
      long endTime = System.currentTimeMillis();
      long latency = endTime - (avgStartTime/Math.max(1, startTimeCnt));
      
      outStage.onNext(latency + "\t" + totalCnt);
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
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
