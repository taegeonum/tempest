package org.edu.snu.tempest.utils;


import org.edu.snu.tempest.operators.common.aggregators.CountByKeyAggregator;

public final class IntegerExtractor implements CountByKeyAggregator.KeyExtractor<Integer, Integer> {
  @Override
  public Integer getKey(final Integer value) {
    return value;
  }
}