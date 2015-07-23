package org.edu.snu.tempest.operators.dynamicmts.impl;

import org.apache.reef.tang.annotations.Parameter;
import org.edu.snu.tempest.operators.Timescale;
import org.edu.snu.tempest.operators.common.Aggregator;
import org.edu.snu.tempest.operators.common.NotFoundException;
import org.edu.snu.tempest.operators.common.OutputLookupTable;
import org.edu.snu.tempest.operators.common.impl.DefaultOutputLookupTableImpl;
import org.edu.snu.tempest.operators.common.impl.TimeAndValue;
import org.edu.snu.tempest.operators.dynamicmts.DynamicRelationCube;
import org.edu.snu.tempest.operators.parameters.CachingRate;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * DynamicRelationCubeImpl.
 *
 * It saves outputs and reuses when doing final aggregation.
 */
public final class DynamicRelationCubeImpl<T> implements DynamicRelationCube<T> {
  private static final Logger LOG = Logger.getLogger(DynamicRelationCubeImpl.class.getName());

  private final List<Timescale> timescales;
  private final Aggregator<?, T> finalAggregator;
  private final double cachingRate;
  private final OutputLookupTable<T> table;
  private final GarbageCollector gc;
  private long largestWindowSize;
  private final ConcurrentHashMap<Timescale, Long> timescaleToSavingTimeMap;

  @Inject
  public DynamicRelationCubeImpl(final List<Timescale> timescales,
                                 final Aggregator<?, T> finalAggregator,
                                 @Parameter(CachingRate.class) final double cachingRate,
                                 final long startTime) {
    this.timescales = timescales;
    this.finalAggregator = finalAggregator;
    this.cachingRate = cachingRate;
    this.table = new DefaultOutputLookupTableImpl<>();
    this.gc = new DefaultGarbageCollectorImpl(timescales, table, startTime);
    this.timescaleToSavingTimeMap = new ConcurrentHashMap<>();
    this.largestWindowSize = findLargestWindowSize();
  }

  @Override
  public void savePartialOutput(final long startTime, final long endTime, final T output) {
    table.saveOutput(startTime, endTime, output);
  }

  /**
   * Aggregates partial outputs and produces a final output.
   * After that, it saves the final result and reuse it
   * for other timescale's final aggregation.
   *
   * @param startTime start time of the output
   * @param endTime end time of the output
   * @param ts timescale
   */
  @Override
  public T finalAggregate(final long startTime, final long endTime, final Timescale ts) {
    final long aggStartTime = System.nanoTime();
    final List<T> dependentOutputs = new LinkedList<>();
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

    // save final result if the condition is satisfied.
    if ((endTime - latestSaveTime) >= ts.windowSize * cachingRate
        && ts.windowSize < this.largestWindowSize) {
      LOG.log(Level.FINE, "SavingRate: " + cachingRate + ", OWO of " + ts +
          " saves output of [" + startTime + "-" + endTime + "]");
      table.saveOutput(startTime, endTime, finalResult);
      timescaleToSavingTimeMap.put(ts, endTime);
    }

    // remove stale outputs.
    gc.onNext(endTime);
    return finalResult;
  }

  @Override
  public void onTimescaleAddition(final Timescale ts, final long startTime) {
    LOG.log(Level.INFO, "addTimescale " + ts);
    synchronized (this.timescales) {
      gc.onTimescaleAddition(ts, startTime);
      this.timescales.add(ts);
      if (ts.windowSize > this.largestWindowSize) {
        this.largestWindowSize = ts.windowSize;
      }
    }
  }

  @Override
  public void onTimescaleDeletion(final Timescale ts) {
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
