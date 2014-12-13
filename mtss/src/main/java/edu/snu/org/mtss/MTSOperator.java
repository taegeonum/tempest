package edu.snu.org.mtss;

public interface MTSOperator<K, V> {
  
  public void addData(K key, V value);
  public void flush(long time) throws Exception;
  public long tickTime();
}
