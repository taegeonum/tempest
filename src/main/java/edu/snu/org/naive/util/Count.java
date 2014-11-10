package edu.snu.org.naive.util;

import edu.snu.org.naive.ReduceFunc;

import java.io.Serializable;

/**
 * Count reduce function
 */
public class Count implements ReduceFunc<Integer>, Serializable {
  @Override
  public Integer compute(Integer value, Integer sofar) {
    return value + sofar;
  }
}