package edu.snu.org.naive.util;

/**
 * Container class for getting value and timestamp.
 */
public class ValueAndTimestamp <V> {

  private final V value;
  private final long timestamp;

  public ValueAndTimestamp(V value, long timestamp) {
    this.value = value;
    this.timestamp = timestamp;
  }

  public V getValue(){
    return value;
  }

  public long getTimestamp(){
    return timestamp;
  }
}
