package edu.snu.org.mtss.bolt;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

/*
 * Multi time-scale operator
 */
public class MTSOperatorBolt extends BaseRichBolt {

  private final Map<Integer, Integer> windowMap;
  
  public MTSOperatorBolt(Map<Integer, Integer> windowMap) {
    this.windowMap = windowMap;
  }
  
  @Override
  public void execute(Tuple paramTuple) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void prepare(Map paramMap, TopologyContext paramTopologyContext,
      OutputCollector paramOutputCollector) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer paramOutputFieldsDeclarer) {
    // TODO Auto-generated method stub
    
  }

}
