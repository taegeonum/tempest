package atc.operator.window.aggregator.impl;

import edu.snu.tempest.operator.window.aggregator.impl.SumAndCount;

import javax.inject.Inject;

/**
 * Created by taegeonum on 9/25/15.
 */
public final class SumAndCountComputeFunc implements ComputeByKeyAggregator.ComputeByKeyFunc<SumAndCount> {

  @Inject
  private SumAndCountComputeFunc() {

  }

  @Override
  public SumAndCount init() {
    return new SumAndCount(0, 0);
  }

  @Override
  public SumAndCount compute(final SumAndCount oldVal, final SumAndCount newVal) {
    return SumAndCount.add(oldVal, newVal);
  }
}
