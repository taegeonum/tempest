package edu.snu.tempest.operators.dynamicmts;

import edu.snu.tempest.operators.dynamicmts.impl.DynamicMTSOperatorImpl;
import edu.snu.tempest.operators.staticmts.MTSOperator;
import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * Dynamic MTS operator interface.
 * It dynamically produces multi-time scale outputs
 * whenever it receives a new timescale or removes an existing timescale.
 */
@DefaultImplementation(DynamicMTSOperatorImpl.class)
public interface DynamicMTSOperator<I> extends TimescaleSignalListener, MTSOperator<I> {

}
