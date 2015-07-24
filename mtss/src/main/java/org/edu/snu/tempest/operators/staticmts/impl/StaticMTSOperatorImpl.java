package org.edu.snu.tempest.operators.staticmts.impl;

import org.apache.reef.tang.annotations.Parameter;
import org.edu.snu.tempest.operators.Timescale;
import org.edu.snu.tempest.operators.common.Aggregator;
import org.edu.snu.tempest.operators.common.Clock;
import org.edu.snu.tempest.operators.common.impl.DefaultMTSClockImpl;
import org.edu.snu.tempest.operators.common.impl.DefaultOverlappingWindowOperatorImpl;
import org.edu.snu.tempest.operators.parameters.InitialStartTime;
import org.edu.snu.tempest.operators.staticmts.MTSOperator;
import org.edu.snu.tempest.operators.staticmts.SlicedWindowOperator;
import org.edu.snu.tempest.operators.staticmts.StaticRelationGraph;

import javax.inject.Inject;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class StaticMTSOperatorImpl<I, V> implements MTSOperator<I> {
  private static final Logger LOG = Logger.getLogger(StaticMTSOperatorImpl.class.getName());

  private final AtomicBoolean started = new AtomicBoolean(false);
  private final Clock clock;
  private final SlicedWindowOperator<I> slicedWindow;

  @Inject
  public StaticMTSOperatorImpl(final Aggregator<I, V> aggregator,
                               final List<Timescale> timescales,
                               final OutputHandler<V> handler,
                               @Parameter(InitialStartTime.class) final long startTime) {
    final StaticRelationGraph<V> relationCube = new StaticRelationGraphImpl<>(timescales, aggregator, startTime);
    this.slicedWindow = new StaticSlicedWindowOperatorImpl<>(aggregator,
        relationCube, startTime);
    this.clock = new DefaultMTSClockImpl(slicedWindow);
    for (final Timescale ts : timescales) {
      final DefaultOverlappingWindowOperatorImpl<V> owo = new DefaultOverlappingWindowOperatorImpl<>(
          ts, relationCube, handler, startTime);
      clock.subscribe(owo);
    }
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
  public void close() throws Exception {
    clock.close();
  }
}
