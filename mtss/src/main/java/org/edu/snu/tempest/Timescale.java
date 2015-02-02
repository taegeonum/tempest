package org.edu.snu.tempest;

import java.io.Serializable;
import java.security.InvalidParameterException;
import java.util.concurrent.TimeUnit;

public class Timescale implements Comparable, Serializable {
  /**
   * Represent timescale
   * Window size and interval 
   * Unit: sec
   */
  private static final long serialVersionUID = 439658002747284570L;
  public final long windowSize;
  public final long intervalSize;
  
  public Timescale(final int windowSize, final int intervalSize, final TimeUnit windowTimeUnit, final TimeUnit intervalTimeUnit) {
    
    if (windowSize <= 0 || windowSize - intervalSize < 0) {
      throw new InvalidParameterException("Invalid window or interval size: " + "(" + windowSize + ", " + intervalSize + ")");
    }
    
    this.windowSize = windowTimeUnit.toSeconds(windowSize);
    this.intervalSize = intervalTimeUnit.toSeconds(intervalSize);
  }
  
  public Timescale(final int windowSize, final int intervalSize) {
    this(windowSize, intervalSize, TimeUnit.SECONDS, TimeUnit.SECONDS);
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
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (int) (intervalSize ^ (intervalSize >>> 32));
    result = prime * result + (int) (windowSize ^ (windowSize >>> 32));
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Timescale other = (Timescale) obj;
    if (intervalSize != other.intervalSize)
      return false;
    if (windowSize != other.windowSize)
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "[" + windowSize + ", " + intervalSize + "]";
  }
}