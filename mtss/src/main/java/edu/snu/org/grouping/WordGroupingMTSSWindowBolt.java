package edu.snu.org.grouping;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.impl.ThreadPoolStage;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import edu.snu.org.TestApp.TimescaleClass;
import edu.snu.org.TestApp.TimescaleList;
import edu.snu.org.mtss.MTSOutput;
import edu.snu.org.mtss.ReduceByKeyComputation;
import edu.snu.org.mtss.ReduceByKeyMTSOperator;
import edu.snu.org.util.ReduceByKeyTuple;
import edu.snu.org.util.Timescale;
import edu.snu.org.util.ValueAndTimestampWithCount;
import edu.snu.org.util.WordCountUtils;
import edu.snu.org.util.WordCountUtils.StartTimeAndTotalCount;
import edu.snu.org.wordcount.ValueAndTimestampWithCountFunc;


/*
 * Calculate word length histogram
 * 
 */

public class WordGroupingMTSSWindowBolt extends BaseRichBolt {

  private final List<Timescale> timescales;
  private ReduceByKeyMTSOperator<Character, ValueAndTimestampWithCount<Long>> mtsOperator;
  private long tickTime;
  private long time;
  private OutputCollector collector;
  private ThreadPoolStage<Collection<MTSOutput<Map<Character, ValueAndTimestampWithCount<Long>>>>> executor;

  @Inject
  public WordGroupingMTSSWindowBolt(@Parameter(TimescaleList.class) TimescaleClass tsClass) throws Exception {
    this.timescales = tsClass.timescales;
    this.tickTime = WordCountUtils.tickTime(timescales);
  }
  
  @Override
  public void execute(Tuple tuple) {
    
    if (isTickTuple(tuple)) {
      time += tickTime; 
      // output 
      try {
        executor.onNext(mtsOperator.flush());
      } catch (Exception e) {
        e.printStackTrace();
      }
    } else {
      String val = (String) tuple.getValue(0);
      val.trim();
      if (val.length() > 0) {
        Character key = val.charAt(0);
        long timestamp = (long)tuple.getValue(2);
        mtsOperator.receiveInput(new ReduceByKeyTuple<>(key, new ValueAndTimestampWithCount<Long>(1L, timestamp, 1)));
        collector.ack(tuple);
      }

    }
  }

  @Override
  public void prepare(Map paramMap, TopologyContext paramTopologyContext,
      final OutputCollector collector) {
    this.collector = collector;
    
    this.executor = new ThreadPoolStage<>("stage", new EventHandler<Collection<MTSOutput<Map<Character, ValueAndTimestampWithCount<Long>>>>>() {

      @Override
      public void onNext(
          Collection<MTSOutput<Map<Character, ValueAndTimestampWithCount<Long>>>> paramT) {
        
        for (MTSOutput<Map<Character, ValueAndTimestampWithCount<Long>>> output : paramT) {
          StartTimeAndTotalCount stc = WordCountUtils.getAverageStartTimeAndTotalCount(output.result);
          collector.emit("size" + output.sizeOfWindow, new Values(output.result, stc.avgStartTime, stc.totalCount));        
        }
      }
      
    }, timescales.size());
    
    try {
      this.mtsOperator = new ReduceByKeyMTSOperator<>(
              timescales, new ReduceByKeyComputation<Character, ValueAndTimestampWithCount<Long>>(new ValueAndTimestampWithCountFunc<Long>(new CountLong())));
      
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  @Override
  public void cleanup() {
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    for (Timescale ts : timescales) {
      declarer.declareStream("size" + ts.windowSize, new Fields("result", "avgTimestamp", "totalCnt"));
    }
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    Map<String, Object> conf = new HashMap<String, Object>();
    conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, tickTime);
    return conf;
  }
  
  
  public static boolean isTickTuple(Tuple tuple) {
    return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID) && tuple.getSourceStreamId().equals(
        Constants.SYSTEM_TICK_STREAM_ID);
  }
}
