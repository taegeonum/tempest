package org.edu.snu.tempest.operator.impl;

import org.edu.snu.tempest.Timescale;
import org.edu.snu.tempest.operator.*;
import org.edu.snu.tempest.operator.relationcube.RelationCube;
import org.edu.snu.tempest.operator.relationcube.impl.DynamicRelationCubeImpl;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class DynamicMTSOperatorImpl<I, V> implements MTSOperator<I, V> {
  private static final Logger LOG = Logger.getLogger(DynamicMTSOperatorImpl.class.getName());

  private final AtomicBoolean started = new AtomicBoolean(false);
  private final Aggregator<I, V> aggregator;
  private final OutputHandler<V> outputHandler;
  private final Clock clock;
  private final RelationCube<V> relationCube;
  private final SlicedWindowOperator<I> slicedWindow;
  private final List<OverlappingWindowOperator<V>> overlappingWindowOperators;
  private final Map<Timescale, Subscription<Timescale>> subscriptions;
  
  @Inject
  public DynamicMTSOperatorImpl(final Aggregator<I, V> aggregator,
                                final List<Timescale> timescales,
                                final OutputHandler<V> handler) {
    // TODO configure cachingRate
    this.aggregator = aggregator;
    this.outputHandler = handler;
    this.relationCube = new DynamicRelationCubeImpl<>(timescales, aggregator, 0);
    this.subscriptions = new HashMap<>();
    this.overlappingWindowOperators = new LinkedList<>();

    
    this.slicedWindow = new DefaultSlicedWindowOperatorImpl<>(aggregator, timescales, relationCube);
    this.clock = new DefaultMTSClockImpl(slicedWindow, 1L, TimeUnit.SECONDS);

    for (Timescale timescale : timescales) {
      OverlappingWindowOperator<V> owo = new DefaultOverlappingWindowOperatorImpl<V>(timescale, relationCube, outputHandler, new LogicalTime(0));
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
    LOG.log(Level.INFO, "MTSOperator addTimescale: " + ts);

    LogicalTime currentTime = clock.getCurrentTime();
    OverlappingWindowOperator<V> owo = new DefaultOverlappingWindowOperatorImpl<>(ts, relationCube, outputHandler, currentTime);
    this.overlappingWindowOperators.add(owo);

    //1. add timescale to SlicedWindowOperator
    this.slicedWindow.onTimescaleAddition(ts, currentTime);
    this.relationCube.addTimescale(ts, currentTime.logicalTime);
    //2. add overlappingWindow
    Subscription<Timescale> ss = this.clock.subscribe(owo);
    subscriptions.put(ss.getToken(), ss);

  }

  @Override
  public void onTimescaleDeletion(final Timescale ts) {
    // TODO
    LOG.log(Level.INFO, "MTSOperator removeTimescale: " + ts);
    LogicalTime currentTime = clock.getCurrentTime();

    Subscription<Timescale> ss = subscriptions.get(ts);
    if (ss == null) {
      LOG.log(Level.WARNING, "Deletion error: Timescale " + ts + " not exists. ");
    } else {
      this.slicedWindow.onTimescaleDeletion(ts, currentTime);
      this.relationCube.removeTimescale(ts, currentTime.logicalTime);
      ss.unsubscribe();
    }
  }

  @Override
  public void close() throws Exception {
    clock.close();
  }
}
