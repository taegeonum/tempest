package edu.snu.org.naive;

import java.io.Serializable;

import edu.snu.org.util.ReduceFunc;
import edu.snu.org.util.ValueAndTimestamp;

/**
 * Count reduce function
 */
public class Count implements ReduceFunc<ValueAndTimestamp<Integer>>, Serializable {
  @Override
  public ValueAndTimestamp<Integer> compute(ValueAndTimestamp<Integer> newRecord,
                                          ValueAndTimestamp<Integer> sofarRecord) {
    return new ValueAndTimestamp(newRecord.getValue() + sofarRecord.getValue(),
        newRecord.getTimestamp() + sofarRecord.getTimestamp());
  }
}