package edu.snu.org.mtss;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;

import edu.snu.org.mtss.MTSOperator.ComputationLogic;
import edu.snu.org.util.ReduceByKeyTuple;
import edu.snu.org.util.ReduceFunc;

public class ReduceByKeyComputation<K, V> implements ComputationLogic<ReduceByKeyTuple<K, V>, Map<K, V>> {

  private final ReduceFunc<V> redFunc;
  
  @Inject
  public ReduceByKeyComputation(ReduceFunc<V> redFunc) {
    this.redFunc = redFunc;
  }
  
  @Override
  public Map<K, V> computeInitialInput(ReduceByKeyTuple<K, V> input) {
    Map<K, V> map = new HashMap<>();
    map.put(input.key, input.value);
    return map;
  }

  @Override
  public Map<K, V> computeIntermediate(ReduceByKeyTuple<K, V> input,
      Map<K, V> state) {
    
    V oldVal = state.get(input.key);
    V newVal = null;
    if (oldVal == null) {
      newVal = input.value;
    } else {
      newVal = redFunc.compute(oldVal, input.value);
    }
    
    state.put(input.key, newVal);
    return state;
  }

  @Override
  public Map<K, V> computeOutput(Collection<Map<K, V>> states) {
    Map<K, V> map = new HashMap<>();
    for( Map<K, V> state : states) {
      for (Map.Entry<K, V> entry : state.entrySet()) {
        V oldVal = map.get(entry.getKey());
        V newVal = null;
        if (oldVal == null) {
          newVal = entry.getValue();
        } else {
          newVal = redFunc.compute(oldVal, entry.getValue());
        }
        map.put(entry.getKey(), newVal);
      }
    }
    return map;
  }
}
