package edu.snu.org;

import java.util.Map;

import javax.inject.Inject;

import org.apache.reef.tang.annotations.Parameter;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import edu.snu.org.TestApp.InputInterval;
import edu.snu.org.util.InputReader;


/**
 * Read input from files
 */
public class FileReadWordSpout extends BaseRichSpout {


  SpoutOutputCollector _collector;
  private final double sendingInterval;
  private final InputReader reader;
  
  @Inject
  public FileReadWordSpout(@Parameter(InputInterval.class) double sendingInterval, 
      InputReader reader) {
    this.sendingInterval = sendingInterval;
    this.reader = reader;
  }

  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    _collector = collector;
  }

  @Override
  public void nextTuple() {
    
    double point = sendingInterval - ((int)sendingInterval);
    long num = (long)sendingInterval;
    
    int repeated = 1;
    if (point > 0) {
      num += 1;
      repeated = (int)(1.0 / point);
    }
    
    for (int i = 0; i < repeated; i++) {
      String input = reader.nextLine();
      for(String word: input.split(" ")) {
        _collector.emit(new Values(word, 1, System.currentTimeMillis()));
      }
    }
    
    Utils.sleep(num);
  }

  @Override
  public void close() {
    try {
      reader.close();
    } catch (Exception e) {
      e.printStackTrace();
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
