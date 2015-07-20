package org.edu.snu.tempest.operator.relationcube.impl;

import org.apache.log4j.Logger;
import org.edu.snu.tempest.operator.impl.NotFoundException;
import org.edu.snu.tempest.operator.impl.TimeAndValue;
import org.edu.snu.tempest.operator.relationcube.OutputLookupTable;

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
  private final static Logger LOG = Logger.getLogger(DefaultOutputLookupTableImpl.class.getName());
  
  public final ConcurrentMap<Long, TreeMap<Long, V>> table;
  
  @Inject
  public DefaultOutputLookupTableImpl() {
    table = new ConcurrentHashMap<Long, TreeMap<Long, V>>();
  }
  
  @Override
  public void saveOutput(final long startTime, final long endTime, final V output) {
    table.putIfAbsent(startTime, new TreeMap<Long, V>(new LongComparator()));
    
    TreeMap<Long, V> row = table.get(startTime);
    row.put(endTime, output);
  }

  @Override
  public V lookup(final long startTime, final long endTime) throws NotFoundException {
    V entry;
    try {
      entry = table.get(startTime).get(endTime);
    } catch (Exception e) {
      throw new NotFoundException("Cannot find element: at (" + startTime + ", " + endTime + ")");
    }
    
    return entry;
  }

  @Override
  public TreeMap<Long, V> lookup(final long startTime) {
    return table.get(startTime);
  }

  @Override
  public TimeAndValue<V> lookupLargestSizeOutput(final long startTime, final long endTime) throws NotFoundException {
    TreeMap<Long, V> row = table.get(startTime);
    
    if (row == null) {
      throw new NotFoundException("Cannot lookup startTime " + startTime + " in lookupLargestSizeOutput");
    }
    
    Long largestKey = row.floorKey(endTime);
    if (largestKey == null) {
      throw new NotFoundException("Cannot lookup endTime " + endTime  + "from " + startTime + " in lookupLargestSizeOutput");
    } else {
      return new TimeAndValue<V>(startTime, largestKey, row.get(largestKey));
    }
  }

  @Override
  public void deleteRow(final long startTime) {
    table.remove(startTime);
  }

  class LongComparator implements Comparator<Long> {

    @Override
    public int compare(Long o1, Long o2) {
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
