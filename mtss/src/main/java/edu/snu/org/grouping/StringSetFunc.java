package edu.snu.org.grouping;

import java.util.Set;

import edu.snu.org.util.ReduceFunc;

public class StringSetFunc implements ReduceFunc<Set<String>> {

  @Override
  public Set<String> compute(Set<String> value, Set<String> sofar) {
    value.addAll(sofar);
    return value;
  }

}
