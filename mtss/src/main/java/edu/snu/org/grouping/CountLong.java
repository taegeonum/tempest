package edu.snu.org.grouping;

import edu.snu.org.util.ReduceFunc;

public class CountLong implements ReduceFunc<Long> {

  @Override
  public Long compute(Long a, Long b) {
    return a + b;
  }
}
