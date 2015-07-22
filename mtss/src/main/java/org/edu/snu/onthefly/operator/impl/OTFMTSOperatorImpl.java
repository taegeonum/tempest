package org.edu.snu.onthefly.operator.impl;

import org.edu.snu.tempest.Timescale;
import org.edu.snu.tempest.operator.Clock;
import org.edu.snu.tempest.operator.MTSOperator;
import org.edu.snu.tempest.operator.impl.DefaultMTSClockImpl;
import org.edu.snu.tempest.operator.impl.DynamicMTSOperatorImpl;
import org.edu.snu.tempest.operator.relationcube.GarbageCollector;

import javax.inject.Inject;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * "On-the-fly sharing for streamed aggregation"'s operator
 * for multi-time scale sliding window operator.
 */
public final class OTFMTSOperatorImpl<I, V> implements MTSOperator<I, V> {
  private static final Logger LOG = Logger.getLogger(DynamicMTSOperatorImpl.class.getName());

  private final AtomicBoolean started = new AtomicBoolean(false);
  private final Aggregator<I, V> aggregator;
  private final Clock clock;
  private final OTFSlicedWindowOperatorImpl<I, V> slicedWindow;
  
  @Inject
  public OTFMTSOperatorImpl(final Aggregator<I, V> aggregator,
                            final List<Timescale> timescales,
                            final OutputHandler<V> handler,
                            final Long startTime) {
    this.aggregator = aggregator;

    this.slicedWindow = new OTFSlicedWindowOperatorImpl<>(aggregator, handler, timescales, startTime);
    this.clock = new DefaultMTSClockImpl(slicedWindow);
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
  public void onTimescaleAddition(final Timescale ts, final long startTime) {
    LOG.log(Level.INFO, "MTSOperator addTimescale: " + ts);

    //1. change scliedWindow
    this.slicedWindow.onTimescaleAddition(ts, startTime);
  }

  @Override
  public void onTimescaleDeletion(final Timescale ts) {
    // TODO
    LOG.log(Level.INFO, "MTSOperator removeTimescale: " + ts);
  }

  @Override
  public void close() throws Exception {
    clock.close();
    slicedWindow.close();
  }
  
  class GarbageCollectorImpl implements GarbageCollector {

    @Override
    public void onTimescaleAddition(Timescale ts, long startTime) {

    }

    @Override
    public void onTimescaleDeletion(Timescale ts) {
      
    }

    @Override
    public void onNext(Long aLong) {

    }
  }
}