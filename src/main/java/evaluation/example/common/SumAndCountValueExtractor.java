package evaluation.example.common;

import backtype.storm.tuple.Tuple;
import edu.snu.tempest.operator.window.aggregator.impl.SumAndCount;
import edu.snu.tempest.operator.window.aggregator.impl.ValueExtractor;

import javax.inject.Inject;

/**
 * Created by taegeonum on 9/25/15.
 */
public final class SumAndCountValueExtractor implements ValueExtractor<Tuple, SumAndCount> {

  @Inject
  private SumAndCountValueExtractor() {
  }


  @Override
  public SumAndCount getValue(final Tuple input) {
    return new SumAndCount(Long.getLong(input.getString(0).split("\t")[1]), 1);
  }
}
