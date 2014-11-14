package edu.snu.org.mtss;

import java.io.Serializable;

import edu.snu.org.util.ReduceFunc;

public class Count implements ReduceFunc<Integer>, Serializable {
  
  /**
   * 
   */
  private static final long serialVersionUID = 8738490432071082927L;

  public Integer compute(Integer value, Integer sofar) {
    return value + sofar;
  }
}
