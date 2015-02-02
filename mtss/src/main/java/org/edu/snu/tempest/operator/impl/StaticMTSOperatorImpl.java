package org.edu.snu.tempest.operator.impl;

import org.edu.snu.tempest.Timescale;
import org.edu.snu.tempest.operator.Clock;
import org.edu.snu.tempest.operator.MTSOperator;
import org.edu.snu.tempest.operator.SlicedWindowOperator;
import org.edu.snu.tempest.operator.Subscription;
import org.edu.snu.tempest.operator.relationcube.RelationCube;
import org.edu.snu.tempest.operator.relationcube.impl.StaticRelationCubeImpl;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class StaticMTSOperatorImpl<I, V> implements MTSOperator<I, V> {
  private static final Logger LOG = Logger.getLogger(StaticMTSOperatorImpl.class.getName());

  private final AtomicBoolean started = new AtomicBoolean(false);
  private final OutputHandler<V> outputHandler;
  private final Clock clock;
  private final RelationCube<V> relationCube;
  private final SlicedWindowOperator<I> slicedWindow;
  private final List<DefaultOverlappingWindowOperatorImpl<V>> overlappingWindowOperators;
  private final Map<Timescale, Subscription<Timescale>> subscriptions;
  private final List<Timescale> timescales;

  @Inject
  public StaticMTSOperatorImpl(final Aggregator<I, V> aggregator,
                               final List<Timescale> timescales,
                               final OutputHandler<V> handler) {
    this.outputHandler = handler;
    this.relationCube = new StaticRelationCubeImpl<V>(timescales, aggregator);
    this.subscriptions = new HashMap<>();
    this.overlappingWindowOperators = new LinkedList<>();
    
    this.timescales = timescales;

    this.slicedWindow = new DefaultSlicedWindowOperatorImpl<>(aggregator, timescales, relationCube);
    this.clock = new DefaultMTSClockImpl(slicedWindow, 1L, TimeUnit.SECONDS);

    Collections.sort(timescales);

    for (Timescale ts : timescales) {
      DefaultOverlappingWindowOperatorImpl<V> owo = new DefaultOverlappingWindowOperatorImpl<>(ts, relationCube, outputHandler, new LogicalTime(0));
      this.overlappingWindowOperators.add(owo);
      Subscription<Timescale> ss = clock.subscribe(owo);
      subscriptions.put(ss.getToken(), ss);
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
  public void onTimescaleAddition(final Timescale ts) {
    // do nothing
    throw new RuntimeException("Not support onTimescaleAddition");
  }

  @Override
  public void onTimescaleDeletion(final Timescale ts) {
    // do nothing
    throw new RuntimeException("Not support onTimescaleDeletion");
  }

  @Override
  public void close() throws Exception {
    clock.close();
  }
}
