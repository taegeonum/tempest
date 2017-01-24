package atc.operator.window.aggregator.impl;

import javax.inject.Inject;

/**
 * Created by taegeonum on 9/25/15.
 */
public final class LinregFunc implements ComputeByKeyAggregator.ComputeByKeyFunc<LinregVal> {

  @Inject
  private LinregFunc() {

  }

  @Override
  public LinregVal init() {
    return new LinregVal(0, 0, 0, 0, 0);
  }

  @Override
  public LinregVal compute(final LinregVal oldVal, final LinregVal newVal) {
    return LinregVal.add(oldVal, newVal);
  }
}
