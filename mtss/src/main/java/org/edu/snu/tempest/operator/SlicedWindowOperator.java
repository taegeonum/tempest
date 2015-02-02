package org.edu.snu.tempest.operator;

import org.apache.reef.wake.EventHandler;
import org.edu.snu.tempest.operator.impl.LogicalTime;

import org.edu.snu.tempest.Timescale;

/**
 * Sliced window operator [ reference: On-the-fly sharing for streamed aggregation ].
 * It chops input stream and aggregates the input using Aggregator.
 * After that, it saves the partially aggregated results into RelationCube.
 *
 * The method how to chop input stream is introduced by "on-the-fly sharing for streamed aggregation".
 */
public interface SlicedWindowOperator<I> extends EventHandler<LogicalTime> {

  /*
   * Aggregate input
   */
  public void execute(final I val);

  public void onTimescaleAddition(Timescale ts, LogicalTime time);
  public void onTimescaleDeletion(Timescale ts, LogicalTime time);
}
