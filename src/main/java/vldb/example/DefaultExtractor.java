package vldb.example;


import vldb.operator.window.aggregator.impl.KeyExtractor;

import javax.inject.Inject;

/**
 * Created by taegeonum on 4/22/16.
 */
public final class DefaultExtractor implements KeyExtractor<Object, Object> {

  @Inject
  private DefaultExtractor() {

  }

  @Override
  public Object getKey(final Object input) {
    return input;
  }
}
