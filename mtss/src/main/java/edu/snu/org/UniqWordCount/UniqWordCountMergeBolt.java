package edu.snu.org.UniqWordCount;

import java.util.Map;

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

public class UniqWordCountMergeBolt extends BaseBasicBolt {

  private final OutputWriter writer;
  private final int numOfInputBolts;
  private ThreadPoolStage<String> outStage;
  private int count;
  private long avgStartTime;
  private long totalCnt;
  private int startTimeCnt;
  private long results = 0;

  
  @Inject
  public UniqWordCountMergeBolt(
      @Parameter(NumBolt.class) final int numOfInputBolts, 
      OutputWriter writer) {
    
    this.numOfInputBolts = numOfInputBolts;
    this.writer = writer;
    
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
    int uniqCnt = (int) tuple.getValue(0);
    long avgSt = (long)tuple.getLong(1);
    avgStartTime += avgSt;
    
    if (avgSt != 0) {
      startTimeCnt++;
    }
    
    totalCnt += (long)tuple.getLong(2);
    

    // merge
    results += uniqCnt;

    if (count == numOfInputBolts) {
      // flush 
      count = 0;
      System.out.println("Uniq: " + results);
      //collector.emit(new Values(list));
      long endTime = System.currentTimeMillis();
      long latency = endTime - (avgStartTime/Math.max(1, startTimeCnt));
      
      outStage.onNext(latency + "\t" + totalCnt);
      results = 0;
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
