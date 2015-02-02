package org.edu.snu.tempest.examples.storm.wordcount;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.Map;
import java.util.Random;
import java.util.logging.Logger;

/**
 * Sends randomly selected words continuously
 */
public class RandomWordSpout extends BaseRichSpout {

  private static final int DEFAULT_SENDING_INTERVAL = 1;
  private static final Logger LOG = Logger.getLogger(RandomWordSpout.class.getName());
  
  SpoutOutputCollector _collector;
  Random _rand;
  private final int sendingInterval;
  private final Random random = new Random();
  
  @Inject
  public RandomWordSpout(@Parameter(WordCountTest.InputInterval.class) double sendingInterval) {
    this.sendingInterval = (int) sendingInterval;
  }

  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    _collector = collector;
    _rand = new Random();
  }

  @Override
  public void nextTuple() {
    Utils.sleep(sendingInterval);
    for (int i = 0; i < 5; i++) {
      _collector.emit(new Values(getRandomWord(), 1, System.currentTimeMillis()));
    }
  }

  @Override
  public void ack(Object id) {
  }

  @Override
  public void fail(Object id) {
    throw new RuntimeException();
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("word", "count", "timestamp"));
  }
  
  private String getRandomWord() {
    char[] word = new char[4]; // words of length 3 through 10. (1 and 2 letter words are boring.)
    for(int j = 0; j < word.length; j++)
    {
        word[j] = (char)('a' + random.nextInt(20));
    }
    
    return new String(word);
  }
}
