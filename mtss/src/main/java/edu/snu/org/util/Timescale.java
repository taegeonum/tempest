package edu.snu.org.util;

import java.io.Serializable;
import java.security.InvalidParameterException;
import java.util.concurrent.TimeUnit;

public class Timescale implements Comparable, Serializable {
  /**
   * Represent timescale
   * Window size and interval 
   */
  private static final long serialVersionUID = 439658002747284570L;
  public final long windowSize;
  public final long intervalSize;
  public final TimeUnit windowTimeUnit;
  public final TimeUnit intervalTimeUnit;
  
  public Timescale(final int windowSize, final int intervalSize, final TimeUnit windowTimeUnit, final TimeUnit intervalTimeUnit) {
    
    if (windowSize <= 0 || windowSize - intervalSize < 0) {
      throw new InvalidParameterException("Invalid window or interval size: " + "(" + windowSize + ", " + intervalSize + ")");
    }
    
    this.windowSize = windowTimeUnit.toSeconds(windowSize);
    this.intervalSize = intervalTimeUnit.toSeconds(intervalSize);
    this.windowTimeUnit = windowTimeUnit;
    this.intervalTimeUnit = intervalTimeUnit;
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
    return "[" + windowSize + ", " + intervalSize + "]";
  }
}