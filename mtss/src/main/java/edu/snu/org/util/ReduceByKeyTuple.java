package edu.snu.org.util;

public class ReduceByKeyTuple<K, V> {

  public final K key;
  public final V value;
  
  public ReduceByKeyTuple(K key, V value) {
    this.key = key;
    this.value = value;
  }
}
