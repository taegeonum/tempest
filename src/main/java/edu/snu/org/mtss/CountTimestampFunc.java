package edu.snu.org.mtss;

import edu.snu.org.util.ReduceFunc;
import edu.snu.org.util.ValueAndTimestamp;

public class CountTimestampFunc<V> implements ReduceFunc<ValueAndTimestamp<V>> {

  private final ReduceFunc<V> reduceFunc;
  
  public CountTimestampFunc(ReduceFunc<V> reduceFunc) {
    this.reduceFunc = reduceFunc;
  }

  @Override
  public ValueAndTimestamp<V> compute(ValueAndTimestamp<V> value,
      ValueAndTimestamp<V> sofar) {
    return new ValueAndTimestamp<>(reduceFunc.compute(value.getValue(), sofar.getValue()), value.getTimestamp() + sofar.getTimestamp());
  }

}
