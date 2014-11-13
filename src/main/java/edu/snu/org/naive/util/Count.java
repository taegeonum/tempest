package edu.snu.org.naive.util;

import edu.snu.org.naive.ReduceFunc;

import java.io.Serializable;

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