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
import org.edu.snu.tempest.examples.storm.parameters.InputInterval;

import javax.inject.Inject;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Sends randomly selected words which have zipfian distribution continuously.
 */
public final class ZipfianWordSpout extends BaseRichSpout {
  private static final int DEFAULT_SENDING_INTERVAL = 1;
  private static final Logger LOG = Logger.getLogger(ZipfianWordSpout.class.getName());
  
  SpoutOutputCollector collector;
  ZipfianGenerator rand;
  private final int sendingInterval;
  
  @Inject
  public ZipfianWordSpout(@Parameter(InputInterval.class) final double sendingInterval) {
    this.sendingInterval = (int) sendingInterval;
  }

  @Override
  public void open(final Map conf,
                   final TopologyContext context,
                   final SpoutOutputCollector col) {
    LOG.log(Level.INFO, ZipfianWordSpout.class.getName() + " started");
    this.collector = col;
    rand = new ZipfianGenerator(15 * 15 * 15 * 15, 1.4);
  }

  @Override
  public void nextTuple() {
    Utils.sleep(sendingInterval);
    for (int i = 0; i < 5; i++) {
      this.collector.emit(new Values(rand.nextString(), 1, System.currentTimeMillis()));
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
  public void declareOutputFields(final OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("word", "count", "timestamp"));
  }
}
