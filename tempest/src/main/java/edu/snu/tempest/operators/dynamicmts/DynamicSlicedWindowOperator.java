package edu.snu.tempest.operators.dynamicmts;

import edu.snu.tempest.operators.staticmts.SlicedWindowOperator;

/**
 * DynamicSliced window operator [ reference: On-the-fly sharing for streamed aggregation ].
 * It chops input stream and aggregates the input using Aggregator.
 * After that, it saves the partially aggregated results into RelationCube.
 *
 * The method how to chop input stream is introduced by "on-the-fly sharing for streamed aggregation".
 */
public interface DynamicSlicedWindowOperator<I> extends SlicedWindowOperator<I>, TimescaleSignalListener {

}
