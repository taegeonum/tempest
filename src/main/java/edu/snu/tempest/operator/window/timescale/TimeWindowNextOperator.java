package edu.snu.tempest.operator.window.timescale;

import edu.snu.tempest.operator.Operator;

/**
 * This operator is called after timescale window operation.
 * It receives timescale window output.
 */
public interface TimeWindowNextOperator<I, O> extends
    Operator<TimescaleWindowOutput<I>, TimescaleWindowOutput<O>> {
}
