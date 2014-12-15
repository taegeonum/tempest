package edu.snu.org.UniqWordCount;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
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
import edu.snu.org.grouping.StringSetFunc;
import edu.snu.org.mtss.DefaultMTSOperator;
import edu.snu.org.mtss.MTSOperator;
import edu.snu.org.mtss.MTSOutput;
import edu.snu.org.util.Timescale;
import edu.snu.org.util.ValueAndTimestampWithCount;
import edu.snu.org.util.WordCountUtils;
import edu.snu.org.util.WordCountUtils.StartTimeAndTotalCount;
import edu.snu.org.wordcount.ValueAndTimestampWithCountFunc;
import edu.snu.org.wordcount.WindowBoltParameter.SlideInterval;
import edu.snu.org.wordcount.WindowBoltParameter.WindowLength;

public class UniqWordCountNaiveWindowBolt extends BaseRichBolt{

  private static final Logger LOG = Logger.getLogger(UniqWordCountNaiveWindowBolt.class);

  private final List<Timescale> timescales;
  private MTSOperator<ValueAndTimestampWithCount<String>, ValueAndTimestampWithCount<Set<String>>> mtsOperator;
  private long tickTime;
  private long time;
  private OutputCollector collector;
  private ThreadPoolStage<Collection<MTSOutput<ValueAndTimestampWithCount<Set<String>>>>> executor;


  /*
   * windowLength, slideInterval is millisecond
   */
  @Inject
  public UniqWordCountNaiveWindowBolt(@Parameter(WindowLength.class) int windowLength, 
      @Parameter(SlideInterval.class) int slideInterval) {
    
    timescales = new LinkedList<>();
    timescales.add(new Timescale(windowLength, slideInterval, TimeUnit.SECONDS, TimeUnit.SECONDS));
    this.tickTime = WordCountUtils.tickTime(timescales);

    LOG.log(Level.INFO, "UniqWordCountNaiveWindowBolt naive window bolt created: " + windowLength + " , " + slideInterval);
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
          collector.emit(new Values(output.result.value.size(), output.result.timestamp, output.result.count));
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
    declarer.declare(new Fields("countMap", "averageTimestamp", "totalCount"));
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