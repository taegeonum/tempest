package edu.snu.org.mtss;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

public class Timescale implements Comparable, Serializable {
  /**
   * Represent timescale
   * Window size and interval 
   */
  private static final long serialVersionUID = 439658002747284570L;
  private final long windowSize;
  private final long intervalSize;
  private final TimeUnit windowTimeUnit;
  private final TimeUnit intervalTimeUnit;
  
  public Timescale(int windowSize, int intervalSize, TimeUnit windowTimeUnit, TimeUnit intervalTimeUnit) {
    this.windowSize = windowTimeUnit.toSeconds(windowSize);
    this.intervalSize = intervalTimeUnit.toSeconds(intervalSize);
    this.windowTimeUnit = windowTimeUnit;
    this.intervalTimeUnit = intervalTimeUnit;
  }
  
  public long getWindowSize() { return windowSize; }
  public long getIntervalSize() { return intervalSize; }
  
  public TimeUnit getWindowSizeTimeUnit() {
    return windowTimeUnit;
  }
  
  public TimeUnit getIntervalTimeUnit() {
    return intervalTimeUnit;
  }

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
  
  @Override
  public String toString() {
    return "[" + getWindowSize() + ", " + getIntervalSize() + "]";
  }
}