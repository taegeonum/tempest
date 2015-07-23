package org.edu.snu.naive.operator.impl;


import org.edu.snu.onthefly.operator.impl.OTFRelationCubeImpl;
import org.edu.snu.tempest.operators.Timescale;
import org.edu.snu.tempest.operators.common.Aggregator;
import org.edu.snu.tempest.operators.common.Clock;
import org.edu.snu.tempest.operators.common.OverlappingWindowOperator;
import org.edu.snu.tempest.operators.common.impl.DefaultMTSClockImpl;
import org.edu.snu.tempest.operators.common.impl.DefaultOverlappingWindowOperatorImpl;
import org.edu.snu.tempest.operators.dynamicmts.DynamicSlicedWindowOperator;
import org.edu.snu.tempest.operators.dynamicmts.impl.DynamicMTSOperatorImpl;
import org.edu.snu.tempest.operators.dynamicmts.impl.DynamicSlicedWindowOperatorImpl;
import org.edu.snu.tempest.operators.staticmts.MTSOperator;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Execute just one timescale window operator.
 */
public final class NaiveWindowOperator<I, V> implements MTSOperator<I> {
  private static final Logger LOG = Logger.getLogger(DynamicMTSOperatorImpl.class.getName());

  private final AtomicBoolean started = new AtomicBoolean(false);
  private final Aggregator<I, V> aggregator;
  private final OutputHandler<V> outputHandler;
  private final Clock clock;
  private final OTFRelationCubeImpl<V> relationCube;
  private final DynamicSlicedWindowOperator<I> slicedWindow;

  @Inject
  public NaiveWindowOperator(final Aggregator<I, V> aggregator,
                             final Timescale timescale,
                             final OutputHandler<V> handler,
                             final Long startTime) throws Exception {
    List<Timescale> timescales = new LinkedList<>();
    timescales.add(timescale);
    this.aggregator = aggregator;
    this.outputHandler = handler;
    this.relationCube = new OTFRelationCubeImpl<>(timescales, aggregator, startTime);
    this.slicedWindow = new DynamicSlicedWindowOperatorImpl<>(aggregator, timescales,
        relationCube, startTime);
    this.clock = new DefaultMTSClockImpl(slicedWindow);
    final OverlappingWindowOperator<V> owo = new DefaultOverlappingWindowOperatorImpl<V>(
        timescale, relationCube, outputHandler, startTime);
    clock.subscribe(owo);
  }

  @Override
  public void start() {
    if (started.compareAndSet(false, true)) {
      LOG.log(Level.INFO, "NaiveMTSOperator start");
      this.clock.start();
    }
  }

  @Override
  public void execute(final I val) {
    LOG.log(Level.FINEST, "NaiveMTSOperator execute : ( " + val + ")");
    this.slicedWindow.execute(val);
  }

  @Override
  public void close() throws Exception {
    clock.close();
  }
}