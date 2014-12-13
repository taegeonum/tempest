package edu.snu.org.mtss;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Stage;
import org.apache.reef.wake.impl.ThreadPoolStage;

import edu.snu.org.mtss.DefaultDependencyTableImpl.Range;
import edu.snu.org.mtss.DependencyTable.DTCell;
import edu.snu.org.util.ReduceFunc;
import edu.snu.org.util.Timescale;

public class DefaultMTSOperator<K, V> implements MTSOperator<K, V> {

  private final ReduceFunc<V> reduceFunc; 
  private Map<K, V> innerMap;
  private final EventHandler<MTSOutput<K, V>> outputHandler;
  private final ThreadPoolStage<MTSOutput<K, V>> executor;
  private final DependencyTable table;
  
  /*
   * Implementation MTS operator 
   *
   * @param timeScales list of timescale. 
   * @param reduceFunc reduce function
   * @param outputHandler output handler 
   */
  public DefaultMTSOperator(List<Timescale> timeScales, 
      ReduceFunc<V> reduceFunc, 
      EventHandler<MTSOutput<K, V>> outputHandler) throws Exception {
    
    if (timeScales.size() == 0) {
      throw new Exception("MTSOperator should have multiple timescales");
    }
    
    
    this.executor = new ThreadPoolStage<>(outputHandler, timeScales.size());
    this.reduceFunc = reduceFunc;
    this.innerMap = new HashMap<>();
    this.outputHandler = outputHandler;
    
    this.table = new DefaultDependencyTableImpl(timeScales);
  }

  
  /*
   * compute data 
   */
  @Override
  public void addData(K key, V value) {
    V oldVal = innerMap.get(key);
    
    if (oldVal == null) {
      innerMap.put(key, value);
    } else {
      innerMap.put(key, reduceFunc.compute(oldVal, value));
    }
  }
  
  /*
   * Flush data
   */
  @Override
  public void flush(long time) throws Exception {
    
    // calculate row
    long row = (time % table.getPeriod()) == 0 ? table.getPeriod() : (time % table.getPeriod());
    Map<Timescale, DTCell> cells = table.row(row); 
    Map<K, V> states = innerMap;
    innerMap = new HashMap<>();
    
    // Each outputNode presents 
    int i = 0;
    for (DTCell cell : cells.values()) {
      
      if (cell.getState() != null) {
        throw new Exception("DTCell state should be null");
      }
      
      // Virtual timescale output node
      if (cell.isVirtualTimescaleCell()) {
        Range r = cell.getRange();
        executor.onNext(new MTSOutput<K, V>(time, r.end - r.start, states));
      } else {
        // create state from dependencies 
        states = null;
        for (DTCell referee : cell.getDependencies()) {
          // When first iteration, the referee of pointed by redline could have null state. We can skip this when first iteration. 
          if (referee.getState() != null) {
            if (states == null) {
              states = new HashMap<>((HashMap<K,V>)referee.getState());
            } else {
              for (Entry<K, V> entry : ((HashMap<K,V>)referee.getState()).entrySet()) {
                V oldVal = states.get(entry.getKey());
                V newVal = null;
                if (oldVal == null) {
                  newVal = entry.getValue();
                } else {
                  newVal = reduceFunc.compute(oldVal, entry.getValue());
                }
                states.put(entry.getKey(), newVal);
              }
            }
          }
        }
        
        // Flush data to output handler
        Range r = cell.getRange();
        outputHandler.onNext(new MTSOutput<K, V>(time, r.end - r.start, states));
      }

      // save the state if other outputNodes reference it
      if (cell.getRefCnt() > 0) {
        cell.setState(states); 
      }
      
      // decrease the dependencies refCnt
      decreaseDependenciesRefCnt(cell);
      i++;
    }
  }


  private void decreaseDependenciesRefCnt(DTCell cell) {

    for (DTCell referee : cell.getDependencies()) {
      if (referee.getState() != null) {
        referee.decreaseRefCnt();
      }
    }
  }

  @Override
  public long tickTime() {
    return table.getBucketSize();
  }
  
  @Override
  public String toString() {
    return table.toString();
  }
}
