package edu.snu.org.naive.bolt;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import edu.snu.org.naive.Count;
import edu.snu.org.naive.SlidingWindow;
import edu.snu.org.util.ValueAndTimestamp;

/**
 * This bolt counts word by window
 */
public class WordCountByWindowBolt extends BaseRichBolt{

  private static final Logger LOG = Logger.getLogger(WordCountByWindowBolt.class);
  private int slideIntervalByBucket;
  private int bucketLength;
  private int bucketNum;
  private int bucketCount;
  private SlidingWindow<String, ValueAndTimestamp<Integer>> slidingWindow;
  private OutputCollector collector;

  /**
   * Calculates the greatest common divider of integer a and b.
   * Assumes that a >= b
   *
   * @param a Larger integer
   * @param b Smaller integer
   */
  private int gcd(int a, int b) {
    if (b == 0) {
      return a;
    }
    int c = a % b;
    return gcd(b, c);
  }

  /*
   * windowLength, slideInterval is millisecond
   */
  public WordCountByWindowBolt(int windowLength, int slideInterval) {
    windowLength = (int)TimeUnit.SECONDS.toMillis(windowLength);
    slideInterval = (int)TimeUnit.SECONDS.toMillis(slideInterval);
    
    bucketLength = gcd(windowLength, slideInterval);
    slideIntervalByBucket = slideInterval/bucketLength;
    bucketNum = windowLength/bucketLength;
    bucketCount = 0;
    slidingWindow = new SlidingWindow(bucketNum, new Count());
  }

  @Override
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    this.collector = collector;
  }

  @Override
  public void execute(Tuple tuple) {
    // Bolt receives 'TICK'
    if(tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID) && tuple.getSourceStreamId().equals(
        Constants.SYSTEM_TICK_STREAM_ID)) {
      slide();
    }
    // Bolt receives normal tuple
    else {
      countAndAck(tuple);
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("countMap", "averageTimestamp", "totalCount"));
  }

  private void slide() {
    bucketCount++;

    // Slides the window and get results
    if (bucketCount % slideIntervalByBucket == 0) {
      Map<String, ValueAndTimestamp<Integer>> reduced = slidingWindow.getResultAndSlideBucket();
      //Map<String, Integer> result = new HashMap<>();
      long totalTimestamp = 0;
      long totalCount = 0;
      for(String key: reduced.keySet()) {
        int count = reduced.get(key).getValue();
        //result.put(key, count);
        totalTimestamp += reduced.get(key).getTimestamp();
        totalCount += count;
      }
      collector.emit(new Values(reduced, totalTimestamp / totalCount, totalCount));
    }
    // Just slides the window
    else {
      slidingWindow.slideBucket();
    }
  }

  private void countAndAck(Tuple tuple) {
    String key = (String) tuple.getValue(0);
    Integer value = (Integer) tuple.getValue(1);
    Long timestamp = (Long) tuple.getValue(2);
    slidingWindow.reduce(key, new ValueAndTimestamp(value, timestamp));
    collector.ack(tuple);
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    Map<String, Object> conf = new HashMap<String, Object>();
    conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, bucketLength/1000);
    return conf;
  }
}