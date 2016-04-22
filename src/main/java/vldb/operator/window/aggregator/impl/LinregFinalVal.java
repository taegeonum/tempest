package vldb.operator.window.aggregator.impl;

/**
 * Created by taegeonum on 10/18/15.
 */
public final class LinregFinalVal {

  public final double slope;
  public final double b;

  public LinregFinalVal(final double slope,
                        final double b) {
    this.slope = slope;
    this.b = b;
  }
}
