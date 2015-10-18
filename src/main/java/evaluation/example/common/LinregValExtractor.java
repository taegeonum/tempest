package evaluation.example.common;

import backtype.storm.tuple.Tuple;
import edu.snu.tempest.operator.window.aggregator.impl.LinregVal;
import edu.snu.tempest.operator.window.aggregator.impl.ValueExtractor;

import javax.inject.Inject;
import java.util.Random;

/**
 * Created by taegeonum on 9/25/15.
 */
public final class LinregValExtractor implements ValueExtractor<Tuple, LinregVal> {

  private final Random random;

  @Inject
  private LinregValExtractor() {
    this.random = new Random();
  }

  @Override
  public LinregVal getValue(final Tuple input) {
    final long x = System.currentTimeMillis();
    final double y = random.nextLong();
    return new LinregVal(x, y, x*y, x*x,1);
  }
}
