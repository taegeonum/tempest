package edu.snu.org.mtss;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.impl.ThreadPoolStage;

import edu.snu.org.mtss.DefaultDependencyTableImpl.Range;
import edu.snu.org.mtss.DependencyTable.DTCell;
import edu.snu.org.util.Timescale;

public class DefaultMTSOperator<I, S> implements MTSOperator<I, S> {

  private S interState;
  private final DependencyTable table;
  private final ComputationLogic<I, S> computationLogic;
  private long time;
  
  /*
   * Implementation MTS operator 
   *
   * @param timeScales list of timescale. 
   * @param reduceFunc reduce function
   * @param outputHandler output handler 
   */
  public DefaultMTSOperator(List<Timescale> timeScales, 
      ComputationLogic<I, S> computationLogic) throws Exception {
    
    if (timeScales.size() == 0) {
      throw new Exception("MTSOperator should have multiple timescales");
    }
    
    this.computationLogic = computationLogic;
    this.table = new DefaultDependencyTableImpl(timeScales);
    this.time = table.getBucketSize();
  }

  
  /*
   * compute data 
   */
  @Override
  public void receiveInput(I input) {
    if (interState == null) {
      interState = computationLogic.computeInitialInput(input);
    } else {
      interState = computationLogic.computeIntermediate(input, interState);
    }
  }
  
  /*
   * Flush data
   */
  @Override
  public Collection<MTSOutput<S>> flush() {
    
    // calculate row
    long row = (time % table.getPeriod()) == 0 ? table.getPeriod() : (time % table.getPeriod());
    time += table.getBucketSize();
    Map<Timescale, DTCell> cells = table.row(row); 
    S output = interState;
    interState = null;
    
    List<MTSOutput<S>> outputList = new LinkedList<>();
    
    // Each outputNode presents 
    int i = 0;
    for (DTCell cell : cells.values()) {
      if (cell.getState() != null) {
        throw new RuntimeException("DTCell state should be null");
      }
      
      List<S> states = new ArrayList<>();
      // create state from dependencies 
      for (DTCell referee : cell.getDependencies()) {
        // When first iteration, the referee of pointed by redline could have null state. We can skip this when first iteration. 
        if (referee.getState() != null) {
          states.add((S)referee.getState());
        }
      }

      // compute output
      if (i != 0) {
        output = computationLogic.computeOutput(states);
      }

      // Flush data to output handler
      Range r = cell.getRange();
      if (!cell.isVirtualTimescaleCell()) {
        //outputHandler.onNext(new MTSOutput<S>(time, r.end - r.start, output));
        outputList.add(new MTSOutput<S>(time, r.end - r.start, output));
      }

      // save the state if other outputNodes reference it
      if (cell.getRefCnt() > 0) {
        cell.setState(output); 
      }
      output = null;

      // decrease the dependencies refCnt
      decreaseDependenciesRefCnt(cell);
      i++;

    }
    return outputList;
  }


  private void decreaseDependenciesRefCnt(DTCell cell) {

    for (DTCell referee : cell.getDependencies()) {
      if (referee.getState() != null) {
        referee.decreaseRefCnt();
      }
    }
  }
  
  @Override
  public String toString() {
    return table.toString();
  }
}
