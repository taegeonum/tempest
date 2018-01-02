package vldb.operator.window.timescale.pafas.active;

import org.apache.reef.tang.annotations.DefaultImplementation;
import vldb.operator.window.timescale.common.FinalAggregator;
import vldb.operator.window.timescale.common.Timespan;

import java.util.List;

/**
 * Created by taegeonum on 8/31/17.
 */
@DefaultImplementation(DefaultActiveFinalAggregatorImpl.class)
public interface ActiveFinalAggregator<V> extends FinalAggregator<V> {

  /**
   * Trigger final aggregations of the final timespans.
   * @param finalTimespans final timespans
   */
  void triggerFinalAggregation(List<Timespan> finalTimespans,
                               long actualTriggerTime, V activePartial);
}
