package org.edu.snu.onthefly.operator.impl;

import org.edu.snu.tempest.operators.Timescale;
import org.edu.snu.tempest.operators.common.Aggregator;
import org.edu.snu.tempest.operators.common.NotFoundException;
import org.edu.snu.tempest.operators.common.OutputLookupTable;
import org.edu.snu.tempest.operators.common.impl.TimeAndValue;
import org.edu.snu.tempest.operators.common.impl.DefaultOutputLookupTableImpl;
import org.edu.snu.tempest.operators.dynamicmts.DynamicRelationCube;
import org.edu.snu.tempest.operators.dynamicmts.impl.DefaultGarbageCollectorImpl;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * OTFRelationCubeImpl.
 *
 * It just saves partial aggregation.
 */
public final class OTFRelationCubeImpl<T> implements DynamicRelationCube<T> {
  private static final Logger LOG = Logger.getLogger(OTFRelationCubeImpl.class.getName());

  private final List<Timescale> timescales;
  private final Aggregator<?, T> finalAggregator;
  private final OutputLookupTable<T> table;
  private final GarbageCollector gc;
  private long largestWindowSize;

  @Inject
  public OTFRelationCubeImpl(final List<Timescale> timescales,
                                 final Aggregator<?, T> finalAggregator,
                                 final long startTime) {
    this.timescales = timescales;
    this.finalAggregator = finalAggregator;
    this.table = new DefaultOutputLookupTableImpl<>();
    this.gc = new DefaultGarbageCollectorImpl(timescales, table, startTime);
    this.largestWindowSize = findLargestWindowSize();
  }

  @Override
  public void savePartialOutput(long startTime, long endTime, T output) {
    table.saveOutput(startTime, endTime, output);
  }

  /**
   * It just produces the final result
   * and does not reuse the result for other timescales.
   *
   * @param startTime start time of the output
   * @param endTime end time of the output
   * @param ts timescale
   * @return
   */
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

    // remove
    gc.onNext(endTime);
    return finalResult;
  }

  @Override
  public void onTimescaleAddition(Timescale ts, long startTime) {
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
  public void onTimescaleDeletion(Timescale ts) {
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
