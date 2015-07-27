package edu.snu.stream.naive.operator.impl;


import edu.snu.stream.onthefly.operator.impl.OTFRelationCubeImpl;
import edu.snu.tempest.operators.Timescale;
import edu.snu.tempest.operators.common.Aggregator;
import edu.snu.tempest.operators.common.MTSOperatorScheduler;
import edu.snu.tempest.operators.common.OverlappingWindowOperator;
import edu.snu.tempest.operators.common.impl.DefaultMTSOperatorSchedulerImpl;
import edu.snu.tempest.operators.common.impl.DefaultOverlappingWindowOperatorImpl;
import edu.snu.tempest.operators.dynamicmts.DynamicSlicedWindowOperator;
import edu.snu.tempest.operators.dynamicmts.impl.DynamicMTSOperatorImpl;
import edu.snu.tempest.operators.dynamicmts.impl.DynamicSlicedWindowOperatorImpl;
import edu.snu.tempest.operators.staticmts.MTSOperator;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * Execute just one timescale window operator.
 * This operator is just for evaluation.
 */
public final class NaiveWindowOperator<I, V> implements MTSOperator<I> {
  private static final Logger LOG = Logger.getLogger(DynamicMTSOperatorImpl.class.getName());

  /**
   * Is this operator started or not.
   */
  private final AtomicBoolean started = new AtomicBoolean(false);

  /**
   * Clock for window operation.
   */
  private final MTSOperatorScheduler mtsScheduler;

  /**
   * Sliced window operator for partial aggregation.
   */
  private final DynamicSlicedWindowOperator<I> slicedWindow;

  /**
   * Execute just one timescale window operator.
   * @param aggregator an aggregator for window aggregation.
   * @param timescale a timescale for window operation.
   * @param handler an output handler for receiving window outputs.
   * @param startTime an initial start time of the operator.
   */
  @Inject
  public NaiveWindowOperator(final Aggregator<I, V> aggregator,
                             final Timescale timescale,
                             final MTSOutputHandler<V> handler,
                             final Long startTime) {
    final List<Timescale> timescales = new LinkedList<>();
    timescales.add(timescale);
    final OTFRelationCubeImpl<V> relationCube = new OTFRelationCubeImpl<>(timescales, aggregator, startTime);
    this.slicedWindow = new DynamicSlicedWindowOperatorImpl<>(aggregator, timescales,
        relationCube, startTime);
    this.mtsScheduler = new DefaultMTSOperatorSchedulerImpl(slicedWindow);
    final OverlappingWindowOperator<V> owo = new DefaultOverlappingWindowOperatorImpl<V>(
        timescale, relationCube, handler, startTime);
    mtsScheduler.subscribe(owo);
  }

  /**
   * Start window operation.
   */
  @Override
  public void start() {
    if (started.compareAndSet(false, true)) {
      LOG.log(Level.INFO, "NaiveMTSOperator start");
      this.mtsScheduler.start();
    }
  }

  /**
   * Aggregate input and produce partially aggregated outputs.
   * @param val input value
   */
  @Override
  public void execute(final I val) {
    LOG.log(Level.FINEST, "NaiveMTSOperator execute : ( " + val + ")");
    this.slicedWindow.execute(val);
  }

  @Override
  public void close() throws Exception {
    mtsScheduler.close();
  }
}