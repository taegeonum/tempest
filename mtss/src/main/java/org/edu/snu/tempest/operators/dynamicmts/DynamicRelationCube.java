package org.edu.snu.tempest.operators.dynamicmts;

import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.wake.EventHandler;
import org.edu.snu.tempest.operators.common.RelationCube;
import org.edu.snu.tempest.operators.dynamicmts.impl.DynamicRelationCubeImpl;
import org.edu.snu.tempest.operators.dynamicmts.signal.TimescaleSignalListener;

/**
 * Dynamic RelationCube.
 * It saves final aggregation results according to caching rate.
 *
 */
@DefaultImplementation(DynamicRelationCubeImpl.class)
public interface DynamicRelationCube<T> extends RelationCube<T>, TimescaleSignalListener {
  /**
   * GarbageCollector interface.
   *
   * It periodically removes OutputLookupTable rows in which startTime < currentTime - largestWindowSize
   * It receives LogicalTime tick from Clock
   */
  public interface GarbageCollector extends TimescaleSignalListener, EventHandler<Long> {

  }
}
