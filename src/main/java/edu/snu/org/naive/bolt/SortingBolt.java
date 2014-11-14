package edu.snu.org.naive.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

/**
 * Aggregates all counts and sorts by values
 */
public class SortingBolt extends BaseRichBolt {

  private int numOfInputBolts;
  private int count;
  private Map<String, Integer> results;
  private long averageTimestamp;
  private long totalWordCount;
  private BufferedWriter out;
  private OutputCollector collector;
  private String boltName;

  public SortingBolt(String boltName, int numOfInputBolts) {
    this.numOfInputBolts = numOfInputBolts;
    count = 0;
    results = new HashMap<>();
    averageTimestamp = 0;
    totalWordCount = 0;
    this.boltName = boltName;
  }

  private <K, V extends Comparable<? super V>> Map<K, V> sortByValue( Map<K, V> map )
  {
    List<Map.Entry<K, V>> list =
        new LinkedList<>( map.entrySet() );
    Collections.sort( list, new Comparator<Map.Entry<K, V>>()
    {
      public int compare( Map.Entry<K, V> o1, Map.Entry<K, V> o2 )
      {
        return (o2.getValue()).compareTo( o1.getValue() );
      }
    } );

    Map<K, V> result = new LinkedHashMap<K, V>();
    for (Map.Entry<K, V> entry : list)
    {
      result.put( entry.getKey(), entry.getValue() );
    }
    return result;
  }

  @Override
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    this.collector = collector;
    try {
      out = new BufferedWriter(new FileWriter(boltName + ".txt"));
    } catch (IOException e) {

    }
  }

  /**
   * Aggregate partially counted results
   * @param tuple
   */
  public void aggregate(Tuple tuple) {
    Map<String, Integer> partialResult = (HashMap) tuple.getValue(0);
    long partialAverageTimestamp = (Long) tuple.getValue(1);
    long partialWordCount = (Long) tuple.getValue(2);

    for (String key: partialResult.keySet()) {
      results.put(key, partialResult.get(key));
    }
    double ratio = (double) totalWordCount / (double) partialWordCount;
    averageTimestamp = (long) ((ratio * averageTimestamp + partialAverageTimestamp) / (ratio + 1));
    totalWordCount += partialWordCount;
  }

  @Override
  public void execute(Tuple tuple) {
    count++;
    aggregate(tuple);
    if (count == numOfInputBolts) {
      Map<String, Integer> sortedResults = sortByValue(results);
      long latency = System.currentTimeMillis() - averageTimestamp;
      collector.emit(new Values(sortedResults, latency, totalWordCount));
      try {
        for (String key : sortedResults.keySet()) {
          out.write(key + "\t" + sortedResults.get(key) + "\n");
        }
        out.write("latency: " + latency + "\ttotalCnt: " + totalWordCount + "\n");
        out.flush();
      } catch(IOException e) {

      }
      count = 0;
      results = new HashMap<>();
      averageTimestamp = 0;
      totalWordCount = 0;
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("rankings", "latency", "totalCnt"));
  }

}
