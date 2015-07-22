package org.edu.snu.tempest.operators.dynamicmts;

import org.edu.snu.tempest.operators.staticmts.MTSOperator;
import org.edu.snu.tempest.operators.dynamicmts.signal.TimescaleSignalListener;

/**
 * MTS operator interface.
 * It dynamically produces multi-time scale outputs
 * whenever it receives a new timescale or removes an existing timescale.
 */
public interface DynamicMTSOperator<I> extends TimescaleSignalListener, MTSOperator<I> {

}
