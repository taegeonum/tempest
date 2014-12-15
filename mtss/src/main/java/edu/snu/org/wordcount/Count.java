package edu.snu.org.wordcount;

import java.io.Serializable;

import edu.snu.org.util.ReduceFunc;
import edu.snu.org.util.ValueAndTimestampWithCount;

/**
 * Count reduce function
 */
public class Count implements ReduceFunc<ValueAndTimestampWithCount<Integer>>, Serializable {
  @Override
  public ValueAndTimestampWithCount<Integer> compute(ValueAndTimestampWithCount<Integer> newRecord,
                                          ValueAndTimestampWithCount<Integer> sofarRecord) {
    return new ValueAndTimestampWithCount(newRecord.value + sofarRecord.value,
        newRecord.timestamp + sofarRecord.timestamp, newRecord.count + sofarRecord.count);
  }
}