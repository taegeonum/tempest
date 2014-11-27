package edu.snu.org.mtss;

import java.util.Map;

public class MTSOutput<K, V> {

  public final long time;
  public final long sizeOfWindow;
  public final Map<K, V> result;
  
  public MTSOutput(final long time, final long sizeOfWindow, final Map<K, V> result) {
    this.time = time;
    this.sizeOfWindow = sizeOfWindow;
    this.result = result;
  }
  
  @Override
  public String toString() {
    return "{" + time + ", " + sizeOfWindow + ", " + result + "}";
  }
}
