package edu.snu.org.util;


public class ValueAndTimestamp<V> {
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