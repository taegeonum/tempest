package edu.snu.org.naive.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import edu.snu.org.naive.ReduceFunc;
import edu.snu.org.naive.util.Count;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Aggregates all counts and sorts by values
 */
public class SortingBolt extends BaseBasicBolt {

  private int numOfInputBolts;
  private int count;
  private ConcurrentSkipListMap<String, Integer> results;
  private ReduceFunc<Integer> countFunc;

  SortingBolt(int numOfInputBolts) {
    this.numOfInputBolts = numOfInputBolts;
    count = 0;
    countFunc = new Count();
    results = new ConcurrentSkipListMap<>();
  }

  class ValueComparator implements Comparator<String> {

    Map<String, Integer> base;
    public ValueComparator(Map<String, Integer> base) {
      this.base = base;
    }

    // Note: this comparator imposes orderings that are inconsistent with equals.
    public int compare(String a, String b) {
      if (base.get(a) >= base.get(b)) {
        return -1;
      } else {
        return 1;
      } // returning 0 would merge keys
    }
  }

  /**
   * Aggregate results in a thread-safe way
   * @param tuple
   */
  public void aggregate(Tuple tuple) {
    HashMap<String, Integer> partialResult = (HashMap) tuple.getValue(0);
    for (String key: partialResult.keySet()) {
      Integer val = partialResult.get(key);
      Integer oldVal = results.get(key);
      boolean succ = false;
      do {
        if (oldVal == null) {
          succ = (null == (oldVal = results.putIfAbsent(key, val)));
          if (succ) {
            break;
          }
        } else {
          Integer newVal = countFunc.compute(oldVal, val);
          succ = results.replace(key, oldVal, newVal);
          if (!succ)
            oldVal = results.get(key);
          else {
            break;
          }
        }
      } while (true);
    }
  }

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    count++;
    aggregate(tuple);
    if (count == numOfInputBolts) {
      ValueComparator comparator = new ValueComparator(results);
      TreeMap<String, Integer> sorted_results = new TreeMap<String, Integer>(comparator);
      sorted_results.putAll(results);
      count = 0;
      results = new ConcurrentSkipListMap<>();
      collector.emit(new Values(sorted_results));
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("rankings"));
  }

}
