package edu.snu.org.wordcount;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

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
import edu.snu.org.TestApp.TopN;
import edu.snu.org.util.OutputWriter;
import edu.snu.org.util.ValueAndTimestampWithCount;

/**
 * Aggregates all counts and sorts by values
 */
public class TotalRankingsBolt extends BaseBasicBolt {
  
  private final int numOfInputBolts;
  private int count;
  private final int topN;
  private SortedSet<WordcountTuple> results;
  private long avgStartTime;
  private long totalCnt;
  private int startTimeCnt;
  private final OutputWriter writer;
  private ThreadPoolStage<String> outStage;
  
  @Inject
  public TotalRankingsBolt(@Parameter(TopN.class) final int topN, 
      @Parameter(NumBolt.class) final int numOfInputBolts, 
      OutputWriter writer) {
    this.numOfInputBolts = numOfInputBolts;
    this.topN = topN;
    this.writer = writer;
    
    count = 0;
    avgStartTime = 0;
    totalCnt = 0;
    results = new TreeSet<>();
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
    Map<String, ValueAndTimestampWithCount<Integer>> aggWordCnt = (Map) tuple.getValue(0);
    long avgSt = (long)tuple.getLong(1);
    avgStartTime += avgSt;
    
    if (avgSt != 0) {
      startTimeCnt++;
    }
    
    totalCnt += (long)tuple.getLong(2);
    
    // sort
    for (Map.Entry<String, ValueAndTimestampWithCount<Integer>> entry : aggWordCnt.entrySet()) {
      WordcountTuple wcTuple = new WordcountTuple(entry.getKey(), entry.getValue().value);
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
      long latency = endTime - (avgStartTime/Math.max(1, startTimeCnt));
      
      System.out.println(results);
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