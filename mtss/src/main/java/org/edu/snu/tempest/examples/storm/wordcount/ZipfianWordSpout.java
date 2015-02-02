package org.edu.snu.tempest.examples.storm.wordcount;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.yahoo.ycsb.generator.ZipfianGenerator;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Sends randomly selected words continuously
 */
public class ZipfianWordSpout extends BaseRichSpout {

  private static final int DEFAULT_SENDING_INTERVAL = 1;
  private static final Logger LOG = Logger.getLogger(ZipfianWordSpout.class.getName());
  
  SpoutOutputCollector _collector;
  ZipfianGenerator _rand;
  private final int sendingInterval;
  
  @Inject
  public ZipfianWordSpout(@Parameter(WordCountTest.InputInterval.class) double sendingInterval) {
    this.sendingInterval = (int) sendingInterval;
  }

  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    LOG.log(Level.INFO, ZipfianWordSpout.class.getName() + " started");
    _collector = collector;
    _rand = new ZipfianGenerator(15 * 15 * 15 * 15, 1.4);
  }

  @Override
  public void nextTuple() {
    Utils.sleep(sendingInterval);
    for (int i = 0; i < 5; i++) {
      _collector.emit(new Values(_rand.nextString(), 1, System.currentTimeMillis()));
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
}
