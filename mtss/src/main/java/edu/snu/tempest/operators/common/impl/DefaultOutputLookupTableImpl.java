package edu.snu.tempest.operators.common.impl;

import edu.snu.tempest.operators.common.OutputLookupTable;
import org.apache.log4j.Logger;
import edu.snu.tempest.operators.common.NotFoundException;

import javax.inject.Inject;
import java.util.Comparator;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * It saves outputs into Table.
 * 
 * TODO: consider concurrency
 */
public final class DefaultOutputLookupTableImpl<V> implements OutputLookupTable<V> {
  private static final Logger LOG = Logger.getLogger(DefaultOutputLookupTableImpl.class.getName());

  /**
   * A data structure for saving outputs.
   */
  public final ConcurrentMap<Long, TreeMap<Long, V>> table;
  
  @Inject
  public DefaultOutputLookupTableImpl() {
    table = new ConcurrentHashMap<Long, TreeMap<Long, V>>();
  }

  /**
   * Save an output into the table.
   * @param startTime start time of the output
   * @param endTime end time of the output
   * @param output an output
   */
  @Override
  public void saveOutput(final long startTime, final long endTime, final V output) {
    table.putIfAbsent(startTime, new TreeMap<Long, V>(new LongComparator()));

    final TreeMap<Long, V> row = table.get(startTime);
    row.put(endTime, output);
  }

  /**
   * Lookup an output ranging from startTime to endTime.
   * @param startTime start time of the output
   * @param endTime end time of the output
   * @return an output
   * @throws NotFoundException
   */
  @Override
  public V lookup(final long startTime, final long endTime) throws NotFoundException {
    final V entry;
    try {
      entry = table.get(startTime).get(endTime);
    } catch (final Exception e) {
      throw new NotFoundException("Cannot find element: at (" + startTime + ", " + endTime + ")");
    }
    
    return entry;
  }

  /**
   * Lookup multiple outputs which start at the startTime.
   * @param startTime start time of the outputs
   * @return outputs which start at the startTime.
   * @throws NotFoundException
   */
  @Override
  public TreeMap<Long, V> lookup(final long startTime) throws NotFoundException {
    return table.get(startTime);
  }

  /**
   * Lookup an output having largest endTime within outputs which start at startTime.
   * @param startTime minimum start time
   * @param endTime maximum end time
   * @return an output containing time information (startTime, endTime).
   * @throws NotFoundException
   */
  @Override
  public TimeAndOutput<V> lookupLargestSizeOutput(final long startTime, final long endTime) throws NotFoundException {
    final TreeMap<Long, V> row = table.get(startTime);
    
    if (row == null) {
      throw new NotFoundException("Cannot lookup startTime " + startTime + " in lookupLargestSizeOutput");
    }

    final Long largestKey = row.floorKey(endTime);
    if (largestKey == null) {
      throw new NotFoundException(
          "Cannot lookup endTime " + endTime  + "from " + startTime + " in lookupLargestSizeOutput");
    } else {
      return new TimeAndOutput<V>(startTime, largestKey, row.get(largestKey));
    }
  }

  /**
   * Delete outputs which start at startTime.
   * @param startTime start time of the outputs.
   */
  @Override
  public void deleteOutputs(final long startTime) {
    table.remove(startTime);
  }

  class LongComparator implements Comparator<Long> {
    @Override
    public int compare(final Long o1, final Long o2) {
      if (o1 - o2 < 0) {
        return -1;
      } else if (o1 - o2 > 0) {
        return 1;
      } else {
        return 0;
      }
    }
    
  }
}
