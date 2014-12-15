package edu.snu.org.UniqWordCount;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import edu.snu.org.mtss.DefaultMTSOperator;
import edu.snu.org.mtss.MTSOperator;
import edu.snu.org.mtss.MTSOutput;
import edu.snu.org.util.Timescale;
import edu.snu.org.util.ValueAndTimestampWithCount;
import edu.snu.org.util.WordCountUtils;

public class UniqWordCountMTSSWindowBolt extends BaseRichBolt {

  private final List<Timescale> timescales;
  private MTSOperator<ValueAndTimestampWithCount<String>, ValueAndTimestampWithCount<Set<String>>> mtsOperator;
  private long tickTime;
  private long time;
  private OutputCollector collector;
  private ThreadPoolStage<Collection<MTSOutput<ValueAndTimestampWithCount<Set<String>>>>> executor;

  @Inject
  public UniqWordCountMTSSWindowBolt(@Parameter(TimescaleList.class) TimescaleClass tsClass) throws Exception {
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
      String word = (String) tuple.getValue(0);
      int val = (int) tuple.getValue(1);
      long timestamp = (long)tuple.getValue(2);
      mtsOperator.receiveInput(new ValueAndTimestampWithCount<>(word, timestamp, val));
      collector.ack(tuple);
      

    }
  }

  @Override
  public void prepare(Map paramMap, TopologyContext paramTopologyContext,
      final OutputCollector collector) {
    this.collector = collector;
    
    this.executor = new ThreadPoolStage<>("stage", new EventHandler<Collection<MTSOutput<ValueAndTimestampWithCount<Set<String>>>>>() {

      @Override
      public void onNext(
          Collection<MTSOutput<ValueAndTimestampWithCount<Set<String>>>> paramT) {
        
        for (MTSOutput<ValueAndTimestampWithCount<Set<String>>> output : paramT) {
          collector.emit("size" + output.sizeOfWindow, new Values(output.result.value.size(), output.result.timestamp, output.result.count));
        }
      }
      
    }, timescales.size());
    
    try {
      this.mtsOperator = new DefaultMTSOperator<>(
              timescales, new UniqWordLogic());
      
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
