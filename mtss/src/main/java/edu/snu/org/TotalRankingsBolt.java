package edu.snu.org;

import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import javax.inject.Inject;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import edu.snu.org.util.HDFSWriter;
import edu.snu.org.util.ValueAndTimestamp;

/**
 * Aggregates all counts and sorts by values
 */
public class TotalRankingsBolt extends BaseBasicBolt {

  private final int numOfInputBolts;
  private int count;
  private final int topN;
  private SortedSet<WordcountTuple> results;
  private final String name;
  private FileWriter writer;
  private long avgStartTime;
  private long totalCnt;
  private int startTimeCnt;
  private HDFSWriter hdfsWriter;
  private final String folderName;

  @Inject
  public TotalRankingsBolt(final int topN, final int numOfInputBolts, String name, String folderName) {
    this.numOfInputBolts = numOfInputBolts;
    this.topN = topN;
    this.name = name;
    this.folderName = folderName;
    count = 0;
    avgStartTime = 0;
    totalCnt = 0;
    results = new TreeSet<>();
  }
  
  @Override
  public void prepare(Map stormConf, TopologyContext context) {
    try {
      writer = new FileWriter(folderName + name);
      //hdfsWriter = new HDFSWriter(folderName + "/" + name);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    
  }

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    count++;
    Map<String, ValueAndTimestamp<Integer>> aggWordCnt = (Map) tuple.getValue(0);
    long avgSt = (long)tuple.getLong(1);
    avgStartTime += avgSt;
    
    if (avgSt != 0) {
      startTimeCnt++;
    }
    
    totalCnt += (long)tuple.getLong(2);
    
    // sort
    for (Map.Entry<String, ValueAndTimestamp<Integer>> entry : aggWordCnt.entrySet()) {
      WordcountTuple wcTuple = new WordcountTuple(entry.getKey(), entry.getValue().value);
      results.add(wcTuple);
    }

    if (count == numOfInputBolts) {
      // flush 
      count = 0;
      
      List<WordcountTuple> list = new LinkedList<>();
      int i = 0;
      for ( WordcountTuple tup : results) {
        if (i >= topN) {
          break;
        }
        
        list.add(tup);
      }
      //collector.emit(new Values(list));
      long endTime = System.currentTimeMillis();
      long latency = endTime - (avgStartTime/Math.max(1, startTimeCnt));
      
      try {
        writer.write(latency + "\t" + totalCnt + "\n");
        writer.flush();
        //hdfsWriter.write(latency + "\t" + totalCnt + "\n");
        //writer.flush();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      results.clear();
      
      avgStartTime = totalCnt = startTimeCnt = 0;
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("rankings"));
  }
  
  @Override
  public void cleanup() {
    try {
      //writer.write("Cleanup\n");
      writer.close();
      // TODO: copy local file to HDFS
      //hdfsWriter.copyFromLocalFile(new Path("/tmp/storm-app/" + name), new Path(hdfsWriter.getDefaultFSName() + "/storm_result/" + name));
      //hdfsWriter.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  public class WordcountTuple implements Comparable<WordcountTuple> {
    private final String key;
    private final int count;
    
    public WordcountTuple(String key, int count) {
      this.key = key;
      this.count = count;
    }

    @Override
    public int compareTo(WordcountTuple o) {      
      if (count < o.count) {
        return 1;
      } else if (count > o.count){
        return -1;
      } else {
        return 0;
      }
    }
    
    @Override
    public String toString() {
      return "(K: " + key + ", V: " + count + ")";
    }
  }
}