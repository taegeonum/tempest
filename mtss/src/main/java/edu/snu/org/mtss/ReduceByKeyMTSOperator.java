package edu.snu.org.mtss;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import edu.snu.org.util.ReduceByKeyTuple;
import edu.snu.org.util.Timescale;

public class ReduceByKeyMTSOperator<K, V> implements MTSOperator<ReduceByKeyTuple<K, V>, Map<K, V>> {

  private final DefaultMTSOperator<ReduceByKeyTuple<K, V>, Map<K, V>> defaultMTSOperator;
  
  @Inject
  public ReduceByKeyMTSOperator(List<Timescale> timeScales, 
      ComputationLogic<ReduceByKeyTuple<K, V>, Map<K, V>> computationLogic) throws Exception {
    
    if (timeScales.size() == 0) {
      throw new Exception("MTSOperator should have multiple timescales");
    }
    
    
    this.defaultMTSOperator = new DefaultMTSOperator<>(timeScales, computationLogic);
  }
  
  @Override
  public void receiveInput(ReduceByKeyTuple<K, V> input) {
    this.defaultMTSOperator.receiveInput(input);
  }

  @Override
  public Collection<MTSOutput<Map<K, V>>> flush(long time) {
    return this.defaultMTSOperator.flush(time);
  }
  
  @Override
  public String toString() {
    return this.defaultMTSOperator.toString();
  }
}
