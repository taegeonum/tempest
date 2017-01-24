package atc.operator.window.aggregator.impl;

import edu.snu.tempest.operator.window.aggregator.impl.LinregFinalVal;

/**
 * Created by taegeonum on 9/25/15.
 */
public final class LinregVal {

  public final long x;
  public final double y;
  public final double xy;
  public final double xx;
  public final long n;

  public LinregVal(final long x,
                   final double y,
                   final double xy,
                   final double xx,
                   final long n
                   ) {
    this.x = x;
    this.y = y;
    this.xy = xy;
    this.xx = xx;
    this.n = n;
  }

  public LinregFinalVal getAB() {
    //Log the value of sumX, sumXX etc, and y=ax+b.
    //LOG.log(Level.INFO, "n : " + n + " sumX :" + sumX + " sumXX : "
    //    +sumXX+" sumY : " + sumY + " sumXY : " +sumXY);

    if((n*xx - x*x)==0) {
      return new LinregFinalVal(0, 0);
    }


    double slope = (n*xy - x*y) / (n*xx - x*x);
    double b = (xx*y - xy*x) / (n*xx - x*x);
    //LOG.log(Level.INFO, "y = (" + a + ") * x + (" + b + ")");
    return new LinregFinalVal(slope, b);
  }

  public static LinregVal add(LinregVal a, LinregVal b) {
    return new LinregVal(a.x + b.x, a.y + b.y,
        a.xy + b.xy, a.xx + b.xx, a.n + b.n);
  }
}
