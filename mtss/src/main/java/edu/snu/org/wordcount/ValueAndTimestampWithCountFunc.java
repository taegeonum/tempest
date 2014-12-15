package edu.snu.org.wordcount;

import edu.snu.org.util.ReduceFunc;
import edu.snu.org.util.ValueAndTimestampWithCount;

public class ValueAndTimestampWithCountFunc<V> implements ReduceFunc<ValueAndTimestampWithCount<V>> {

  private final ReduceFunc<V> reduceFunc;
  
  public ValueAndTimestampWithCountFunc(ReduceFunc<V> reduceFunc) {
    this.reduceFunc = reduceFunc;
  }

  @Override
  public ValueAndTimestampWithCount<V> compute(ValueAndTimestampWithCount<V> value,
      ValueAndTimestampWithCount<V> sofar) {
    return new ValueAndTimestampWithCount<>(reduceFunc.compute(value.value, sofar.value), value.timestamp + sofar.timestamp, value.count + sofar.count);
  }

}
