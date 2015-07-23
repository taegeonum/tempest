package org.edu.snu.tempest.operators.staticmts.impl;

import org.edu.snu.tempest.operators.common.Aggregator;
import org.edu.snu.tempest.operators.staticmts.SlicedWindowOperator;
import org.edu.snu.tempest.operators.staticmts.StaticRelationGraph;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This implementation is based on "On-the-fly Sharing for Streamed Aggregation" paper
 * It chops input stream into paired sliced window. 
 *
 */
public final class StaticSlicedWindowOperatorImpl<I, V> implements SlicedWindowOperator<I> {

  private static final Logger LOG = Logger.getLogger(StaticSlicedWindowOperatorImpl.class.getName());
  private final Aggregator<I, V> aggregator;
  private final StaticRelationGraph<V> relationGraph;
  private long nextSliceTime;
  private long prevSliceTime;
  private final Object sync = new Object();
  private V innerMap;

  @Inject
  public StaticSlicedWindowOperatorImpl(
      final Aggregator<I, V> aggregator,
      final StaticRelationGraph<V> relationGraph,
      final long startTime) {
    this.aggregator = aggregator;
    this.relationGraph = relationGraph;
    this.prevSliceTime = startTime;
    this.nextSliceTime = relationGraph.nextSliceTime();
    this.innerMap = aggregator.init();
  }

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
        V output = innerMap;
        innerMap = aggregator.init();
        // saves output to RelationCube
        relationGraph.savePartialOutput(prevSliceTime, nextSliceTime, output);
      }
      prevSliceTime = nextSliceTime;
      nextSliceTime = relationGraph.nextSliceTime();
    }
  }

  @Override
  public void execute(final I val) {
    LOG.log(Level.FINE, "SlicedWindow aggregates input of [" +  val + "]");
    synchronized (sync) {
      innerMap = aggregator.partialAggregate(innerMap, val);
    }
  }
}
