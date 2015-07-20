package org.edu.snu.tempest.operator.relationcube.impl;

import org.edu.snu.tempest.Timescale;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;
import org.edu.snu.tempest.operator.MTSOperator;
import org.edu.snu.tempest.operator.impl.LogicalTime;
import org.edu.snu.tempest.operator.impl.NotFoundException;
import org.edu.snu.tempest.operator.impl.TimeAndValue;
import org.edu.snu.tempest.operator.relationcube.GarbageCollector;
import org.edu.snu.tempest.operator.relationcube.OutputLookupTable;
import org.edu.snu.tempest.operator.relationcube.RelationCube;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * DynamicRelationCubeImpl.
 *
 * It saves outputs and reuses when doing final aggregation.
 */
public class DynamicRelationCubeImpl<T> implements RelationCube<T> {
  private static final Logger LOG = Logger.getLogger(DynamicRelationCubeImpl.class.getName());


  @NamedParameter
  public static final class SavingRate implements Name<Double> {}

  private final List<Timescale> timescales;
  private final MTSOperator.Aggregator<?, T> finalAggregator;
  private final double savingRate;
  private final OutputLookupTable<T> table;
  private final GarbageCollector gc;
  private long largestWindowSize;
  private final ConcurrentHashMap<Timescale, Long> timescaleToSavingTimeMap;

  @Inject
  public DynamicRelationCubeImpl(final List<Timescale> timescales,
                                 final MTSOperator.Aggregator<?, T> finalAggregator,
                                 @Parameter(SavingRate.class) final double savingRate) {
    this.timescales = timescales;
    this.finalAggregator = finalAggregator;
    this.savingRate = savingRate;
    this.table = new DefaultOutputLookupTableImpl<>();
    this.gc = new DefaultGarbageCollectorImpl(timescales, table);
    this.timescaleToSavingTimeMap = new ConcurrentHashMap<>();
    this.largestWindowSize = findLargestWindowSize();
  }

  @Override
  public void savePartialOutput(long startTime, long endTime, T output) {
    table.saveOutput(startTime, endTime, output);
  }

  @Override
  public T finalAggregate(long startTime, long endTime, Timescale ts) {
    long aggStartTime = System.nanoTime();
    List<T> dependentOutputs = new LinkedList<>();
    // lookup dependencies
    long start = startTime;
    boolean isFullyProcessed = true;

    // fetch dependent outputs
    while(start < endTime) {
      TimeAndValue<T> elem;
      try {
        elem = table.lookupLargestSizeOutput(start, endTime);
        LOG.log(Level.FINE, ts + " Lookup : (" + start + ", " + endTime + ")");
        if (start == elem.endTime) {
          isFullyProcessed = false;
          break;
        } else {
          dependentOutputs.add(elem.value);
          start = elem.endTime;
        }
      } catch (NotFoundException e) {
        start += 1;
        isFullyProcessed = false;
      }
    }

    if (!isFullyProcessed) {
      LOG.log(Level.WARNING, "The output of " + ts
          + " at " + endTime + " is not fully produced. "
          + "It only happens when the timescale is recently added");
    }

    // aggregates dependent outputs
    T finalResult = finalAggregator.finalAggregate(dependentOutputs);
    LOG.log(Level.FINE, "AGG TIME OF " + ts + ": "
        + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - aggStartTime)
        + " at " + endTime + ", dependent size: " + dependentOutputs.size());
    Long latestSaveTime = timescaleToSavingTimeMap.get(ts);

    if (latestSaveTime == null) {
      latestSaveTime = new Long(0);
    }

    if ((endTime - latestSaveTime) >= ts.windowSize * savingRate
        && ts.windowSize < this.largestWindowSize) {
      // save result to OLT
      LOG.log(Level.FINE, "SavingRate: " + savingRate + ", OWO of " + ts +
          " saves output of [" + startTime + "-" + endTime + "]");
      table.saveOutput(startTime, endTime, finalResult);
      timescaleToSavingTimeMap.put(ts, endTime);
    }

    // remove
    gc.onNext(new LogicalTime(endTime));
    return finalResult;
  }



  @Override
  public void addTimescale(final Timescale ts, final long time) {
    LOG.log(Level.INFO, "addTimescale " + ts);
    synchronized (this.timescales) {
      gc.onTimescaleAddition(ts);
      this.timescales.add(ts);
      if (ts.windowSize > this.largestWindowSize) {
        this.largestWindowSize = ts.windowSize;
      }
    }
  }

  @Override
  public void removeTimescale(final Timescale ts, final long time) {
    LOG.log(Level.INFO, "removeTimescale " + ts);
    synchronized (this.timescales) {
      gc.onTimescaleDeletion(ts);
      this.timescales.remove(ts);
      if (ts.windowSize == this.largestWindowSize) {
        this.largestWindowSize = findLargestWindowSize();
      }
    }
  }

  private long findLargestWindowSize() {
    long result = 0;
    for (Timescale ts : timescales) {
      if (result < ts.windowSize) {
        result = ts.windowSize;
      }
    }
    return result;
  }
}
