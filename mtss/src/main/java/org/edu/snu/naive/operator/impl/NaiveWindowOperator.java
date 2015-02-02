package org.edu.snu.naive.operator.impl;


import org.edu.snu.onthefly.operator.impl.OTFSlicedWindowOperatorImpl;
import org.edu.snu.tempest.Timescale;
import org.edu.snu.tempest.operator.Clock;
import org.edu.snu.tempest.operator.MTSOperator;
import org.edu.snu.tempest.operator.impl.DefaultMTSClockImpl;
import org.edu.snu.tempest.operator.impl.DynamicMTSOperatorImpl;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Execute just one timescale window operator.
 */
public final class NaiveWindowOperator<I, V> implements MTSOperator<I, V> {
  private static final Logger LOG = Logger.getLogger(DynamicMTSOperatorImpl.class.getName());

  private final AtomicBoolean started = new AtomicBoolean(false);
  private final Clock clock;
  private final OTFSlicedWindowOperatorImpl<I, V> slicedWindow;
  private final List<Timescale> timescales;
  @Inject
  public NaiveWindowOperator(final Aggregator<I, V> aggregator,
                             final Timescale timescale,
                             final OutputHandler<V> handler) {
    this.timescales = new LinkedList<>();
    timescales.add(timescale);
    this.slicedWindow = new OTFSlicedWindowOperatorImpl<>(aggregator, handler, timescales);
    this.clock = new DefaultMTSClockImpl(slicedWindow, 1L, TimeUnit.SECONDS, 2, 2);
  }

  @Override
  public void start() {
    if (started.compareAndSet(false, true)) {
      LOG.log(Level.INFO, "MTSOperator start");
      this.clock.start();
    }
  }
  
  @Override
  public void execute(final I val) {
    LOG.log(Level.FINEST, "MTSOperator execute : ( " + val + ")");
    this.slicedWindow.execute(val);
  }

  @Override
  public void onTimescaleAddition(final Timescale ts) {
    // do nothing
  }

  @Override
  public void onTimescaleDeletion(final Timescale ts) {
    // do nothing
  }

  @Override
  public void close() throws Exception {
    clock.close();
    slicedWindow.close();
  }
}