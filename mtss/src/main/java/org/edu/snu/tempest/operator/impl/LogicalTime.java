package org.edu.snu.tempest.operator.impl;


public final class LogicalTime {
  public final long logicalTime;
  
  public LogicalTime(final long logicalTime) {
    this.logicalTime = logicalTime;
  }
  
  @Override
  public String toString() {
    return logicalTime+"";
  }
}
