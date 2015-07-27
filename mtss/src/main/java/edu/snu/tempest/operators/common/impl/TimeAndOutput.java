package edu.snu.tempest.operators.common.impl;

/**
 * This contains time and output.
 * @param <V> val
 */
public final class TimeAndOutput<V> {
  public final long startTime;
  public final long endTime;
  public final V output;

  /**
   * This contains time and output.
   * @param startTime a start time of the output
   * @param endTime a end time of the output
   * @param output an output
   */
  public TimeAndOutput(final long startTime, final long endTime, final V output) {
    this.startTime = startTime;
    this.endTime = endTime;
    this.output = output;
  }
  
  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("[");
    sb.append(startTime);
    sb.append("-");
    sb.append(endTime);
    sb.append("] ");
    sb.append(this.output);
    return sb.toString();
  }
}
