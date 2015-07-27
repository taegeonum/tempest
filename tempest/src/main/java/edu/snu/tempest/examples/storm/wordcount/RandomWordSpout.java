package edu.snu.tempest.examples.storm.wordcount;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import edu.snu.tempest.examples.storm.parameters.InputInterval;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Sends randomly selected words continuously.
 */
public final class RandomWordSpout extends BaseRichSpout {
  private static final Logger LOG = Logger.getLogger(RandomWordSpout.class.getName());

  private SpoutOutputCollector collector;
  private final int sendingInterval;
  private final RandomDataGenerator random = new RandomDataGenerator();
  
  @Inject
  public RandomWordSpout(@Parameter(InputInterval.class) final double sendingInterval) {
    this.sendingInterval = (int) sendingInterval;
  }

  @Override
  public void open(final Map conf,
                   final TopologyContext context,
                   final SpoutOutputCollector col) {
    this.collector = col;
  }

  @Override
  public void nextTuple() {
    Utils.sleep(sendingInterval);
    for (int i = 0; i < 5; i++) {
      this.collector.emit(new Values(random.nextHexString(4), 1, System.currentTimeMillis()));
    }
  }

  @Override
  public void ack(final Object id) {
  }

  @Override
  public void fail(final Object id) {
    throw new RuntimeException();
  }

  @Override
  public void declareOutputFields(final OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("word", "count", "timestamp"));
  }
}
