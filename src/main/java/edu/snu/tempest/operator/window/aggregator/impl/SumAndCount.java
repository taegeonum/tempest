package edu.snu.tempest.operator.window.aggregator.impl;

/**
 * Created by taegeonum on 9/25/15.
 */
public final class SumAndCount {

  public final long sum;
  public final long count;

  public SumAndCount(final long sum,
                     final long count) {
    this.sum = sum;
    this.count = count;
  }

  public double getAverage() {
    return sum / count;
  }

  public static SumAndCount add(SumAndCount a, SumAndCount b) {
    return new SumAndCount(a.sum + b.sum, a.count + b.count);
  }
}
