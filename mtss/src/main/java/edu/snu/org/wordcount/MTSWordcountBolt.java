package edu.snu.org.wordcount;

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

/*
 * Multi time-scale operator
 */
public class MTSWordcountBolt extends BaseRichBolt {

  private final List<Timescale> timescales;
  private ReduceByKeyMTSOperator<String, ValueAndTimestampWithCount<Integer>> mtsOperator;
  private long tickTime;
  private long time;
  private OutputCollector collector;
  private ThreadPoolStage<Collection<MTSOutput<Map<String, ValueAndTimestampWithCount<Integer>>>>> executor;
  
  @Inject
  public MTSWordcountBolt(@Parameter(TimescaleList.class) TimescaleClass tsClass) throws Exception {
    this.timescales = tsClass.timescales;
    this.tickTime = WordCountUtils.tickTime(timescales);

  }
  
  @Override
  public void execute(Tuple tuple) {
    
    if (isTickTuple(tuple)) {
      time += tickTime; 
      // output 
      executor.onNext(mtsOperator.flush());

    } else {
      String key = (String) tuple.getValue(0);
      int count = (int)tuple.getValue(1);
      long timestamp = (long)tuple.getValue(2);
      mtsOperator.receiveInput(new ReduceByKeyTuple<String, ValueAndTimestampWithCount<Integer>>
                    (key,new ValueAndTimestampWithCount<Integer>(count, timestamp, count) ));
      collector.ack(tuple);
    }
  }

  @Override
  public void prepare(Map paramMap, TopologyContext paramTopologyContext,
      final OutputCollector collector) {
    this.collector = collector;

    this.executor = new ThreadPoolStage<>("stage", new EventHandler<Collection<MTSOutput<Map<String, ValueAndTimestampWithCount<Integer>>>>>() {

      @Override
      public void onNext(
          Collection<MTSOutput<Map<String, ValueAndTimestampWithCount<Integer>>>> paramT) {
        
        for (MTSOutput<Map<String, ValueAndTimestampWithCount<Integer>>> output : paramT) {
          StartTimeAndTotalCount stc = WordCountUtils.getAverageStartTimeAndTotalCount(output.result);
          collector.emit("size" + output.sizeOfWindow, new Values(output.result, stc.avgStartTime, stc.totalCount));        
        }
      }
      
    }, timescales.size());
    
    try {
      this.mtsOperator = new ReduceByKeyMTSOperator<>(
              timescales, new ReduceByKeyComputation<String, ValueAndTimestampWithCount<Integer>>(new Count()));
      
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  @Override
  public void cleanup() {
    try {

    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
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
