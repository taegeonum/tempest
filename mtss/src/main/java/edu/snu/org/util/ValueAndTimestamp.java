package edu.snu.org.util;

import java.io.Serializable;


public class ValueAndTimestamp<V> implements Serializable {
  /**
   * 
   */
  private static final long serialVersionUID = -68303184300480338L;
  public final V value;
  public final long timestamp;
  
  public ValueAndTimestamp(V value, long timestamp) {
    this.value = value;
    this.timestamp = timestamp;
  }
}