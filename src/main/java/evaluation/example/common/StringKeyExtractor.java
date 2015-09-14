package evaluation.example.common;

import backtype.storm.tuple.Tuple;
import edu.snu.tempest.operator.window.aggregator.impl.KeyExtractor;

import javax.inject.Inject;

/**
 * Created by taegeonum on 9/14/15.
 */
public final class StringKeyExtractor implements KeyExtractor<Tuple, String> {

  @Inject
  public StringKeyExtractor() {

  }

  @Override
  public String getKey(final Tuple input) {
    return input.getString(0);
  }
}