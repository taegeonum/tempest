package vldb.operator.window.timescale.profiler;

import org.apache.reef.tang.annotations.DefaultImplementation;

@DefaultImplementation(DefaultAggregationCounterImpl.class)
public interface AggregationCounter {

  void incrementPartialAggregation();

  //void incrementFinalAggregation(long endTime, List<Map> mapList);
  void incrementFinalAggregation();
  void incrementFinalAggregation(long num);

  long getNumPartialAggregation();

  long getNumFinalAggregation();
  void stopCount();
}
