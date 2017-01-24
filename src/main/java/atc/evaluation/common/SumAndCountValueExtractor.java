package atc.evaluation.common;

import backtype.storm.tuple.Tuple;
import edu.snu.tempest.operator.window.aggregator.impl.SumAndCount;
import edu.snu.tempest.operator.window.aggregator.impl.ValueExtractor;

import javax.inject.Inject;
import java.util.Random;

/**
 * Created by taegeonum on 9/25/15.
 */
public final class SumAndCountValueExtractor implements ValueExtractor<Tuple, SumAndCount> {

  private final Random random;

  @Inject
  private SumAndCountValueExtractor() {
    this.random = new Random();
  }


  @Override
  public SumAndCount getValue(final Tuple input) {
    return new SumAndCount(random.nextLong(), 1);
  }
}
