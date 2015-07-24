package org.edu.snu.tempest.operators.dynamicmts;

import org.apache.reef.tang.annotations.DefaultImplementation;
import org.edu.snu.tempest.operators.dynamicmts.impl.DynamicMTSOperatorImpl;
import org.edu.snu.tempest.operators.staticmts.MTSOperator;

/**
 * MTS operator interface.
 * It dynamically produces multi-time scale outputs
 * whenever it receives a new timescale or removes an existing timescale.
 */
@DefaultImplementation(DynamicMTSOperatorImpl.class)
public interface DynamicMTSOperator<I> extends TimescaleSignalListener, MTSOperator<I> {

}
