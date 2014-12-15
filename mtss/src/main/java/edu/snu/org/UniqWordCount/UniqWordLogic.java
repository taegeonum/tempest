package edu.snu.org.UniqWordCount;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import edu.snu.org.mtss.MTSOperator.ComputationLogic;
import edu.snu.org.util.ValueAndTimestampWithCount;

public class UniqWordLogic implements ComputationLogic<ValueAndTimestampWithCount<String>, ValueAndTimestampWithCount<Set<String>>> {

  @Override
  public ValueAndTimestampWithCount<Set<String>> computeInitialInput(ValueAndTimestampWithCount<String> input) {
    Set<String> set = new HashSet<>();
    set.add(input.value);
    return new ValueAndTimestampWithCount<>(set, input.timestamp, input.count);
  }

  @Override
  public ValueAndTimestampWithCount<Set<String>> computeIntermediate(ValueAndTimestampWithCount<String> input, ValueAndTimestampWithCount<Set<String>> state) {
    Set<String> set = state.value;
    set.add(input.value);
    return new ValueAndTimestampWithCount<>(set, input.timestamp + state.timestamp, input.count + state.count);
  }

  @Override
  public ValueAndTimestampWithCount<Set<String>> computeOutput(Collection<ValueAndTimestampWithCount<Set<String>>> states) {
    Set<String> result = new HashSet<>();
    long count = 0;
    long ts = 0;
    for (ValueAndTimestampWithCount<Set<String>> set : states) { 
      result.addAll(set.value);
      count += set.count;
      ts += set.timestamp;
    }
    
    return new ValueAndTimestampWithCount<>(result, ts, count);
  }
}
