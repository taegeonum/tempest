package org.edu.snu.onthefly.operator.impl;

import org.edu.snu.tempest.Timescale;
import org.edu.snu.tempest.operator.MTSOperator;
import org.edu.snu.tempest.operator.Clock;
import org.edu.snu.tempest.operator.relationcube.GarbageCollector;
import org.edu.snu.tempest.operator.impl.DefaultMTSClockImpl;
import org.edu.snu.tempest.operator.impl.DynamicMTSOperatorImpl;
import org.edu.snu.tempest.operator.impl.LogicalTime;

import javax.inject.Inject;
import java.util.List;
import java.util.concurrent.TimeUnit;
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
                            final OutputHandler<V> handler) {
    this.aggregator = aggregator;

    this.slicedWindow = new OTFSlicedWindowOperatorImpl<>(aggregator, handler, timescales);
    this.clock = new DefaultMTSClockImpl(slicedWindow, 1L, TimeUnit.SECONDS);
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
    LOG.log(Level.INFO, "MTSOperator addTimescale: " + ts);

    LogicalTime currentTime = clock.getCurrentTime();
    //1. change scliedWindow
    this.slicedWindow.onTimescaleAddition(ts, currentTime);
  }

  @Override
  public void onTimescaleDeletion(final Timescale ts) {
    // TODO
    LOG.log(Level.INFO, "MTSOperator removeTimescale: " + ts);
    LogicalTime currentTime = clock.getCurrentTime();
  }

  @Override
  public void close() throws Exception {
    clock.close();
    slicedWindow.close();
  }
  
  class GarbageCollectorImpl implements GarbageCollector {

    @Override
    public void onTimescaleAddition(Timescale ts) {
      
    }

    @Override
    public void onTimescaleDeletion(Timescale ts) {
      
    }

    @Override
    public void onNext(LogicalTime arg0) {
      
    }
    
  }
}