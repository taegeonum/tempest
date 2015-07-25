package org.edu.snu.tempest.operators.common;

import org.edu.snu.tempest.operators.common.impl.TimeAndValue;

import java.util.TreeMap;

/**
 * OutputLookupTable interface.
 *
 * Data structure for saving output of multi-time scale results.
 */
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
   * @param startTime row of the table
   * @param endTime col of the table
   */
  V lookup(long startTime, long endTime) throws NotFoundException;
  
  /**
   * Lookup multiple outputs which start with the startTime. 
   * @param startTime row of the table
   */
  TreeMap<Long, V> lookup(long startTime);
  
  /**
   * Lookup largest endTime output which starts at startTime until endTime.
   * e.g) if this table has [s=3, e=4] [s=3, e=5], [s=3, e=7] outputs
   *      and a user calls lookupLargestSizeOutput(startTime=3, endTime=8),
   *      then it returns [s=3, e=7] which is biggest endTime at startTime=3
   * @return TimeAndValue this contains value and time information.
   */
  TimeAndValue<V> lookupLargestSizeOutput(long startTime, long endTime) throws NotFoundException;
  
  /**
   * Delete outputs which start from startTime.
   * @param startTime start time of the outputs.
   */
  void deleteOutputs(long startTime);
}
