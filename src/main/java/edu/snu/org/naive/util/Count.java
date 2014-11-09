package edu.snu.org.naive.util;

import edu.snu.org.naive.ReduceFunc;

/**
 * Count reduce function
 */
public class Count implements ReduceFunc<Integer> {
  @Override
  public Integer compute(Integer value, Integer sofar) {
    return value + sofar;
  }
}
