package edu.snu.org.mtss;

import java.util.concurrent.TimeUnit;

public class Timescale implements Comparable {
  private final long windowSize;
  private final long intervalSize;
  
  public Timescale(int windowSize, int intervalSize, TimeUnit windowTimeUnit, TimeUnit intervalTimeUnit) {
    this.windowSize = windowTimeUnit.toMillis(windowSize);
    this.intervalSize = intervalTimeUnit.toMillis(intervalSize);
    
  }
  
  public long getWindowSize() { return windowSize; }
  public long getIntervalSize() { return intervalSize; }

  @Override
  public int compareTo(Object o) {
    Timescale tt = (Timescale)o;
    
    if (windowSize < tt.windowSize) { 
      return -1;
    } else if (windowSize > tt.windowSize) {
      return 1;
    } else {
      return 0;
    }
  }
}