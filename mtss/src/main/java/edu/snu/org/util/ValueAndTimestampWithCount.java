package edu.snu.org.util;

import java.io.Serializable;


public class ValueAndTimestampWithCount<V> implements Serializable {
  /**
   * 
   */
  private static final long serialVersionUID = -68303184300480338L;
  public final V value;
  public final long timestamp;
  public final long count;
  
  public ValueAndTimestampWithCount(V value, long timestamp, long count) {
    this.value = value;
    this.timestamp = timestamp;
    this.count = count;
  }
}