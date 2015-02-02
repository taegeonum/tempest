package org.edu.snu.tempest.operator.relationcube;

import org.apache.reef.wake.EventHandler;
import org.edu.snu.tempest.operator.impl.LogicalTime;
import org.edu.snu.tempest.signal.TimescaleSignalListener;

/**
 * GarbageCollector interface 
 * 
 * It periodically removes OutputLookupTable rows in which startTime < currentTime - largestWindowSize
 * It receives LogicalTime tick from Clock
 */
public interface GarbageCollector extends TimescaleSignalListener, EventHandler<LogicalTime> {

}
