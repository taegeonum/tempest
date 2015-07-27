package edu.snu.tempest.operators.staticmts.impl;

import edu.snu.tempest.operators.common.Aggregator;
import edu.snu.tempest.operators.staticmts.SlicedWindowOperator;
import edu.snu.tempest.operators.staticmts.StaticRelationGraph;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This implementation is based on "On-the-fly Sharing for Streamed Aggregation" paper
 * It chops input stream into paired sliced window.
 */
public final class StaticSlicedWindowOperatorImpl<I, V> implements SlicedWindowOperator<I> {

  private static final Logger LOG = Logger.getLogger(StaticSlicedWindowOperatorImpl.class.getName());

  /**
   * Aggregator for partial aggregation.
   */
  private final Aggregator<I, V> aggregator;

  /**
   * RelationGraph for saving partial aggregation and for next slice time.
   */
  private final StaticRelationGraph<V> relationGraph;

  /**
   * The next slice time to be sliced.
   */
  private long nextSliceTime;

  /**
   * The previous slice time.
   */
  private long prevSliceTime;

  /**
   * Sync object for partial output.
   */
  private final Object sync = new Object();

  /**
   * Partial output.
   */
  private V partialOutput;

  /**
   * StaticSlicedWindowOperatorImpl.
   * @param aggregator an aggregator for partial aggregation
   * @param relationGraph a relation graph for saving outputs of partial aggregation.
   * @param startTime a start time of the mts operator
   */
  @Inject
  public StaticSlicedWindowOperatorImpl(
      final Aggregator<I, V> aggregator,
      final StaticRelationGraph<V> relationGraph,
      final long startTime) {
    this.aggregator = aggregator;
    this.relationGraph = relationGraph;
    this.prevSliceTime = startTime;
    this.nextSliceTime = relationGraph.nextSliceTime();
    this.partialOutput = aggregator.init();
  }

  /**
   * Slice partial aggregation and save the partial aggregation into RelationGraph in order to reuse it.
   * @param currTime current time
   */
  @Override
  public synchronized void onNext(final Long currTime) {
    LOG.log(Level.FINE, "SlicedWindow tickTime " + currTime + ", nextSlice: " + nextSliceTime);
    while (nextSliceTime < currTime) {
      prevSliceTime = nextSliceTime;
      nextSliceTime = relationGraph.nextSliceTime();
    }

    if (nextSliceTime == currTime) {
      LOG.log(Level.FINE, "Sliced : [" + prevSliceTime + "-" + currTime + "]");
      synchronized (sync) {
        // slice
        final V output = partialOutput;
        partialOutput = aggregator.init();
        // saves output to RelationCube
        LOG.log(Level.FINE, "Save partial output : [" + prevSliceTime + "-" + nextSliceTime + "]"
            + ", output: " + output);
        relationGraph.savePartialOutput(prevSliceTime, nextSliceTime, output);
      }
      prevSliceTime = nextSliceTime;
      nextSliceTime = relationGraph.nextSliceTime();
    }
  }

  /**
   * Aggregates input and generate a partial aggregated output.
   * @param val input
   */
  @Override
  public void execute(final I val) {
    LOG.log(Level.FINE, "SlicedWindow aggregates input of [" +  val + "]");
    synchronized (sync) {
      // partial aggregation
      partialOutput = aggregator.partialAggregate(partialOutput, val);
    }
  }
}
