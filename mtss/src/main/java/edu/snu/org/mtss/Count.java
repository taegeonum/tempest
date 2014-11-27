package edu.snu.org.mtss;

import edu.snu.org.util.ReduceFunc;

public class Count implements ReduceFunc<Integer> {

  public Integer compute(Integer value, Integer sofar) {
    return value + sofar;
  }
}
