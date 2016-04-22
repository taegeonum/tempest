package vldb.operator.window.timescale;

import vldb.operator.Operator;

/**
 * This operator is called after timescale window operation.
 * It receives timescale window output as input of the operator and emits timescale window outputs.
 */
public interface TimeWindowOutputHandler<I, O> extends
    Operator<TimescaleWindowOutput<I>, TimescaleWindowOutput<O>> {
}
