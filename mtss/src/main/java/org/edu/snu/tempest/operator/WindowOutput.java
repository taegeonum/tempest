package org.edu.snu.tempest.operator;

import org.edu.snu.tempest.Timescale;

import java.io.Serializable;

/**
 * MTSOperator sends WindowOutput to OutputHandler.
 */
public final class WindowOutput<V> implements Serializable {

  private static final long serialVersionUID = 1L;
  
  public final Timescale timescale;
  public final V output;
  public final long startTime;
  public final long endTime;
  public final long elapsedTime;
  
  public WindowOutput(final Timescale ts,
                      final V output,
                      final long startTime,
                      final long endTime,
                      final long elapsedTime) {
    this.timescale = ts;
    this.output = output;
    this.startTime = startTime;
    this.endTime = endTime;
    this.elapsedTime = elapsedTime;
  }
  
  @Override
  public String toString() {
    return "[w=" + timescale.windowSize + ", i="
        + timescale.intervalSize + ", "
        + startTime + "~" + endTime + "]: " + output;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (int) (endTime ^ (endTime >>> 32));
    result = prime * result + ((output == null) ? 0 : output.hashCode());
    result = prime * result + (int) (startTime ^ (startTime >>> 32));
    result = prime * result + ((timescale == null) ? 0 : timescale.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    WindowOutput other = (WindowOutput) obj;
    if (endTime != other.endTime) {
      return false;
    }
    if (output == null) {
      if (other.output != null) {
        return false;
      }
    } else if (!output.equals(other.output)) {
      return false;
    }
    if (startTime != other.startTime) {
      return false;
    }
    if (timescale == null) {
      if (other.timescale != null) {
        return false;
      }
    } else if (!timescale.equals(other.timescale)) {
      return false;
    }
    return true;
  }

}
