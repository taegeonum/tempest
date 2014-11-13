package edu.snu.org.mtss.bolt;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.reef.wake.EventHandler;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import edu.snu.org.mtss.MTSOperator;
import edu.snu.org.mtss.MTSOutput;
import edu.snu.org.mtss.Timescale;
import edu.snu.org.mtss.util.ValueAndTimestamp;

/*
 * Multi time-scale operator
 */
public class MTSWordcountBolt extends BaseRichBolt {

  private final List<Timescale> timescales;
  private MTSOperator<String, ValueAndTimestamp<Integer>> mtsOperator;
  private long tickTime;
  private long time;
  private OutputCollector collector;
  
  public MTSWordcountBolt(List<Timescale> timescales) throws Exception {
    this.timescales = timescales;
    this.tickTime = MTSOperator.tickTime(timescales);
  }
  
  @Override
  public void execute(Tuple tuple) {
    
    if (isTickTuple(tuple)) {
      time += tickTime; 
      // output 
      try {
        mtsOperator.flush(time);
      } catch (Exception e) {
        e.printStackTrace();
      }
    } else {
      String key = (String) tuple.getValue(0);
      int count = (int)tuple.getValue(1);
      long timestamp = (long)tuple.getValue(2);
      mtsOperator.addData(key, new ValueAndTimestamp<Integer>(count, timestamp));
    }
  }

  @Override
  public void prepare(Map paramMap, TopologyContext paramTopologyContext,
      final OutputCollector collector) {
    // TODO Auto-generated method stub
    this.collector = collector;
    
    try {
      this.mtsOperator = new MTSOperator<String, ValueAndTimestamp<Integer>>(timescales, new CountTimestampFunc<Integer>(new Count()), new EventHandler<MTSOutput<String, ValueAndTimestamp<Integer>>>() {
        @Override
        public void onNext(MTSOutput<String, ValueAndTimestamp<Integer>> data) {
          
          Map<String, ValueAndTimestamp<Integer>> map = data.getResult();
          
          long totalCnt = 0;
          long avgStartTime = 0;
          for (ValueAndTimestamp<Integer> val : map.values()) {
            totalCnt += val.getValue();
            avgStartTime += val.getTimestamp();
          }
          
          avgStartTime /= Math.max(1, totalCnt);
          collector.emit("size" + data.getSizeOfWindow(), new Values(data.getResult(), avgStartTime, totalCnt));
        }
      });
      
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    for (Timescale ts : timescales) {
      declarer.declareStream("size" + ts.getWindowSize(), new Fields("result", "avgTimestamp", "totalCnt"));
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
