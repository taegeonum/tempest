package evaluation.example.common;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import evaluation.example.parameter.InputRate;
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
  private final long inputRate;

  private final long numKey;
  private final double zipfConstant;

  @Inject
  public ZipfianWordSpout(@Parameter(InputRate.class) long inputRate,
                          final long numKey,
                          final double zipfConstant) {
    this.inputRate = inputRate;
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
      Thread.sleep(1);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    final int loop = (int)inputRate / 1000;

    for (int i = 0; i < loop; i++) {
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