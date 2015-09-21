package evaluation.example.common;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import edu.snu.tempest.example.storm.parameter.InputInterval;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Sends randomly selected words continuously
 */
public final class ZipfianWordSpout extends BaseRichSpout {

  private static final int DEFAULT_SENDING_INTERVAL = 1;
  private static final Logger LOG = Logger.getLogger(ZipfianWordSpout.class.getName());
  
  SpoutOutputCollector outputCollector;
  ZipfianGenerator rand;
  private final int sendingInterval;

  private final long numKey;
  private final double zipfConstant;

  @Inject
  public ZipfianWordSpout(@Parameter(InputInterval.class) double sendingInterval,
                          final long numKey,
                          final double zipfConstant) {
    this.sendingInterval = (int) sendingInterval;
    this.numKey = numKey;
    this.zipfConstant = zipfConstant;
  }

  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    LOG.log(Level.INFO, ZipfianWordSpout.class.getName() + " started");
    outputCollector = collector;
    rand = new ZipfianGenerator(numKey, zipfConstant);
  }

  @Override
  public void nextTuple() {
    try {
      Thread.sleep(sendingInterval);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    for (int i = 0; i < 5; i++) {
      outputCollector.emit(new Values(rand.nextString(), 1, System.currentTimeMillis()));
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