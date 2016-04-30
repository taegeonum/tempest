package vldb.operator.window.timescale.profiler;

import org.apache.reef.tang.annotations.DefaultImplementation;

import java.util.List;
import java.util.Map;

@DefaultImplementation(DefaultAggregationCounterImpl.class)
public interface AggregationCounter {

  void incrementPartialAggregation();

  void incrementFinalAggregation(long endTime, List<Map> mapList);

  long getNumPartialAggregation();

  long getNumFinalAggregation();
}
