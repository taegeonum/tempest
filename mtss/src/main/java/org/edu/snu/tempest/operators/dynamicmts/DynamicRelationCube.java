package org.edu.snu.tempest.operators.dynamicmts;

import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.wake.EventHandler;
import org.edu.snu.tempest.operators.Timescale;
import org.edu.snu.tempest.operators.common.RelationCube;
import org.edu.snu.tempest.operators.dynamicmts.impl.DynamicRelationCubeImpl;

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

  /**
   * CachingPolicy for caching multi-time scale outputs.
   */
  public interface CachingPolicy extends TimescaleSignalListener {
    /**
     * Decide to cache or not to cache the output.
     * @param startTime start time of the output
     * @param endTime end time of the output
     * @param ts the timescale of the output
     * @return
     */
    boolean cache(long startTime, long endTime, Timescale ts);
  }
}
