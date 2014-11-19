package edu.snu.org.mtss;

import java.util.Map;

public class MTSOutput<K, V> {

  private final long time;
  private final long sizeOfWindow;
  private final Map<K, V> result;
  
  public MTSOutput(long time, long sizeOfWindow, Map<K, V> result) {
    this.time = time;
    this.sizeOfWindow = sizeOfWindow;
    this.result = result;
  }
  
  public long getTime() { 
    return time;
  }
  
  public long getSizeOfWindow() { 
    return sizeOfWindow;
  }
  
  public Map<K, V> getResult() {
    return result;
  }
  
  @Override
  public String toString() {
    return "{" + time + ", " + sizeOfWindow + ", " + result + "}";
  }
}
