package org.edu.snu.tempest.operator.relationcube;

import org.edu.snu.tempest.operator.impl.NotFoundException;
import org.edu.snu.tempest.operator.impl.TimeAndValue;

import java.util.TreeMap;

/**
 * OutputLookupTable interface.
 * It saves outputs.
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
   * e.g) if this table has [s=3, e=4] [s=3, e=5], [s=3, e=7] outputs and startTime=3, endTime=8, 
   *      then it returns [s=3, e=7] which is biggest endTime at startTime=3
   */
  TimeAndValue<V> lookupLargestSizeOutput(long startTime, long endTime) throws NotFoundException;
  
  /**
   * Delete outputs which start with startTime.
   * @param startTime row of the table
   */
  void deleteRow(long startTime);
  
}
