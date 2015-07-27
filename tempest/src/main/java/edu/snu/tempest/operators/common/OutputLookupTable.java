package edu.snu.tempest.operators.common;

import edu.snu.tempest.operators.common.impl.DefaultOutputLookupTableImpl;
import edu.snu.tempest.operators.common.impl.TimeAndOutput;
import org.apache.reef.tang.annotations.DefaultImplementation;

import java.util.concurrent.ConcurrentSkipListMap;

/**
 * OutputLookupTable interface.
 *
 * Data structure for saving output of multi-time scale results.
 */
@DefaultImplementation(DefaultOutputLookupTableImpl.class)
public interface OutputLookupTable<V> {

  /**
   * Save an output ranging from startTime to endTime.
   * @param startTime start time of the output
   * @param endTime end time of the output 
   * @param output an output
   */
  void saveOutput(long startTime, long endTime, final V output);
  
  /**
   * Lookup an output ranging from startTime to endTime.
   * @param startTime start time of the output
   * @param endTime end time of the output
   * @return an output
   * @throws NotFoundException throws NotFoundException
   * when it cannot find and output ranging from startTime to endTime.
   */
  V lookup(long startTime, long endTime) throws NotFoundException;
  
  /**
   * Lookup multiple outputs which start at the startTime.
   * @param startTime start time of the outputs
   * @return outputs which start at the startTime.
   * @throws NotFoundException throws NotFoundException
   * when it cannot find and output starting at startTime.
   */
  ConcurrentSkipListMap<Long, V> lookup(long startTime) throws NotFoundException;
  
  /**
   * Lookup an output having largest endTime within outputs which start at startTime.
   * e.g) if this table has [s=3, e=4] [s=3, e=5], [s=3, e=7] outputs
   *      and a user calls lookupLargestSizeOutput(startTime=3, endTime=8),
   *      then it returns [s=3, e=7] which is biggest endTime at startTime=3
   * @param startTime minimum start time
   * @param endTime maximum end time
   * @return TimeAndValue this contains value and time information.
   * @throws NotFoundException throws NotFoundException
   * when it cannot find and output ranging from startTime to endTime.
   */
  TimeAndOutput<V> lookupLargestSizeOutput(long startTime, long endTime) throws NotFoundException;
  
  /**
   * Delete outputs which start at startTime.
   * @param startTime start time of the outputs.
   */
  void deleteOutputs(long startTime);
}
