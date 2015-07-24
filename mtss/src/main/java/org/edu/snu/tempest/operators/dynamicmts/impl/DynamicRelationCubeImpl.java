package org.edu.snu.tempest.operators.dynamicmts.impl;

import org.edu.snu.tempest.operators.Timescale;
import org.edu.snu.tempest.operators.common.Aggregator;
import org.edu.snu.tempest.operators.common.NotFoundException;
import org.edu.snu.tempest.operators.common.OutputLookupTable;
import org.edu.snu.tempest.operators.common.impl.DefaultOutputLookupTableImpl;
import org.edu.snu.tempest.operators.common.impl.TimeAndValue;
import org.edu.snu.tempest.operators.dynamicmts.DynamicRelationCube;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * DynamicRelationCubeImpl.
 *
 * It saves final aggregation outputs and reuses when doing final aggregation.
 */
public final class DynamicRelationCubeImpl<T> implements DynamicRelationCube<T> {
  private static final Logger LOG = Logger.getLogger(DynamicRelationCubeImpl.class.getName());

  /**
   * Aggregator for final aggregation.
   */
  private final Aggregator<?, T> finalAggregator;

  /**
   * Table for saving outputs.
   */
  private final OutputLookupTable<T> table;

  /**
   * Garbage collector removing stale outputs.
   */
  private final GarbageCollector gc;

  private final CachingPolicy cachingPolicy;

  @Inject
  public DynamicRelationCubeImpl(final List<Timescale> timescales,
                                 final Aggregator<?, T> finalAggregator,
                                 final CachingPolicy cachingPolicy,
                                 final long startTime) {
    this.finalAggregator = finalAggregator;
    this.table = new DefaultOutputLookupTableImpl<>();
    this.gc = new DefaultGarbageCollectorImpl(timescales, table, startTime);
    this.cachingPolicy = cachingPolicy;
  }

  /**
   * Save partial output.
   *
   * @param startTime start time of the output
   * @param endTime end time of the output
   * @param output output
   */
  @Override
  public void savePartialOutput(final long startTime, final long endTime, final T output) {
    table.saveOutput(startTime, endTime, output);
  }

  /**
   * Produces a final output by Aggregating partial outputs.
   * After that, it saves the final result
   * and reuses it for other timescales' final aggregation
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
      final TimeAndValue<T> elem;
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
    final T finalResult = finalAggregator.finalAggregate(dependentOutputs);
    LOG.log(Level.FINE, "AGG TIME OF " + ts + ": "
        + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - aggStartTime)
        + " at " + endTime + ", dependent size: " + dependentOutputs.size());

    // save final result if the condition is satisfied.
    if (cachingPolicy.cache(startTime, endTime, ts)) {
      LOG.log(Level.FINE, "Saves output of : " + ts +
          "[" + startTime + "-" + endTime + "]");
      table.saveOutput(startTime, endTime, finalResult);
    }

    // remove stale outputs.
    gc.onNext(endTime);
    return finalResult;
  }

  @Override
  public void onTimescaleAddition(final Timescale ts, final long startTime) {
    LOG.log(Level.INFO, "addTimescale " + ts);
    gc.onTimescaleAddition(ts, startTime);
    cachingPolicy.onTimescaleAddition(ts, startTime);
  }

  @Override
  public void onTimescaleDeletion(final Timescale ts) {
    LOG.log(Level.INFO, "removeTimescale " + ts);
    gc.onTimescaleDeletion(ts);
    cachingPolicy.onTimescaleDeletion(ts);
  }
}
