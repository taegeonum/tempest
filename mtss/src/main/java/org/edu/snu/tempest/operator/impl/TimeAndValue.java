package org.edu.snu.tempest.operator.impl;

import java.util.Map;

public final class TimeAndValue<V> {

  public final long startTime;
  public final long endTime;
  public final V value;
  
  public TimeAndValue(final long startTime, final long endTime, final V val) {
    this.startTime = startTime;
    this.endTime = endTime;
    this.value = val;
  }
  
  @Override
  public String toString() {
    return "[" + startTime + "-" + endTime + "] output";
  }
}
