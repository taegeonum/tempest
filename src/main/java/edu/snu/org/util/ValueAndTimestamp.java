package edu.snu.org.util;

import java.io.Serializable;


public class ValueAndTimestamp<V> implements Serializable {
  /**
   * 
   */
  private static final long serialVersionUID = -68303184300480338L;
  private final V value;
  private final long timestamp;
  
  public ValueAndTimestamp(V value, long timestamp) {
    this.value = value;
    this.timestamp = timestamp;
  }
  
  public V getValue() {
    return value;
  }
  
  public long getTimestamp() {
    return timestamp;
  }
}