package edu.snu.org.wordcount;

import javax.inject.Inject;

import edu.snu.org.util.ReduceFunc;
import edu.snu.org.util.ValueAndTimestampWithCount;

public class CountWithTimestampFunc implements ReduceFunc<ValueAndTimestampWithCount<Long>>{

  private final ReduceFunc<Long> reduce;
  
  @Inject
  public CountWithTimestampFunc(ReduceFunc<Long> reduce) {
    this.reduce = reduce;
  }
  
  @Override
  public ValueAndTimestampWithCount<Long> compute(
      ValueAndTimestampWithCount<Long> value,
      ValueAndTimestampWithCount<Long> sofar) {
    return new ValueAndTimestampWithCount<>(reduce.compute(value.value, sofar.value), value.timestamp + sofar.timestamp, value.count + sofar.count);
  }

}
