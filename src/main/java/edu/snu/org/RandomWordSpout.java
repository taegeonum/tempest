package edu.snu.org;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

/**
 * Sends randomly selected words continuously
 */
public class RandomWordSpout extends BaseRichSpout {

  private static final int DEFAULT_SENDING_INTERVAL = 1;

  SpoutOutputCollector _collector;
  Random _rand;
  private final int sendingInterval;

  public RandomWordSpout() {
    this(DEFAULT_SENDING_INTERVAL);
  }

  public RandomWordSpout(int sendingInterval) {
    this.sendingInterval = sendingInterval;
  }

  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    _collector = collector;
    _rand = new Random();
  }

  @Override
  public void nextTuple() {
    Utils.sleep(sendingInterval);
    String[] sentences = new String[]{ "the cow jumped over the moon", "an apple a day keeps the doctor away",
        "four score and seven years ago", "snow white and the seven dwarfs", "i am at two with nature", 
        "At initialization time, if SLF4J suspects that there may be a api vs. binding version mismatch problem, it will emit a warning about the suspected mismatch.", 
        "A major portion of this course is a term project",
        "The goal is to investigate new ideas and solutions", 
        "The goal of these reviews is to help you synthesize the main ideas and concepts presented in each paper"};
    String sentence = sentences[_rand.nextInt(sentences.length)];
    for(String word: sentence.split(" ")) {
      _collector.emit(new Values(word, 1, System.currentTimeMillis()));
    }
  }

  @Override
  public void ack(Object id) {
  }

  @Override
  public void fail(Object id) {
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("word", "count", "timestamp"));
  }
}
