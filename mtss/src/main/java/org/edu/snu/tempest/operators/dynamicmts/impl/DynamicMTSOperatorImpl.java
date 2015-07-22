package org.edu.snu.tempest.operators.dynamicmts.impl;

import org.edu.snu.tempest.operators.Timescale;
import org.edu.snu.tempest.operators.common.Aggregator;
import org.edu.snu.tempest.operators.common.*;
import org.edu.snu.tempest.operators.common.impl.DefaultMTSClockImpl;
import org.edu.snu.tempest.operators.common.impl.DefaultOverlappingWindowOperatorImpl;
import org.edu.snu.tempest.operators.dynamicmts.DynamicMTSOperator;
import org.edu.snu.tempest.operators.dynamicmts.DynamicSlicedWindowOperator;
import org.edu.snu.tempest.operators.dynamicmts.signal.MTSSignalReceiver;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class DynamicMTSOperatorImpl<I, V> implements DynamicMTSOperator<I> {
  private static final Logger LOG = Logger.getLogger(DynamicMTSOperatorImpl.class.getName());

  private final AtomicBoolean started = new AtomicBoolean(false);
  private final Aggregator<I, V> aggregator;
  private final OutputHandler<V> outputHandler;
  private final Clock clock;
  private final DynamicRelationCubeImpl<V> relationCube;
  private final DynamicSlicedWindowOperator<I> slicedWindow;
  private final List<OverlappingWindowOperator<V>> overlappingWindowOperators;
  private final Map<Timescale, Subscription<Timescale>> subscriptions;
  private final MTSSignalReceiver receiver;

  @Inject
  public DynamicMTSOperatorImpl(final Aggregator<I, V> aggregator,
                                final List<Timescale> timescales,
                                final OutputHandler<V> handler,
                                final MTSSignalReceiver receiver,
                                final Long startTime) throws Exception {
    // TODO configure cachingRate
    this.aggregator = aggregator;
    this.outputHandler = handler;
    this.relationCube = new DynamicRelationCubeImpl<>(timescales, aggregator, 0, startTime);
    this.subscriptions = new HashMap<>();
    this.overlappingWindowOperators = new LinkedList<>();
    this.receiver = receiver;
    this.receiver.addTimescaleSignalListener(this);

    this.slicedWindow = new DynamicSlicedWindowOperatorImpl<>(aggregator, timescales,
        relationCube, startTime);
    this.clock = new DefaultMTSClockImpl(slicedWindow);

    for (Timescale timescale : timescales) {
      OverlappingWindowOperator<V> owo = new DefaultOverlappingWindowOperatorImpl<V>(
          timescale, relationCube, outputHandler, startTime);
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
      try {
        this.receiver.start();
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }
  }
  
  @Override
  public void execute(final I val) {
    LOG.log(Level.FINEST, "MTSOperator execute : ( " + val + ")");
    this.slicedWindow.execute(val);
  }

  @Override
  public synchronized void onTimescaleAddition(final Timescale ts, final long startTime) {
    // 1. add overlapping window operator
    LOG.log(Level.INFO, "MTSOperator addTimescale: " + ts);
    OverlappingWindowOperator<V> owo = new DefaultOverlappingWindowOperatorImpl<>(
        ts, relationCube, outputHandler, startTime);
    this.overlappingWindowOperators.add(owo);
    Subscription<Timescale> ss = this.clock.subscribe(owo);
    subscriptions.put(ss.getToken(), ss);

    //2. add timescale to SlicedWindowOperator
    this.slicedWindow.onTimescaleAddition(ts, startTime);

    //3. add timescale to RelationCube.
    this.relationCube.onTimescaleAddition(ts, startTime);
  }

  @Override
  public synchronized void onTimescaleDeletion(final Timescale ts) {
    // TODO: implement timescale deletion.
    LOG.log(Level.INFO, "MTSOperator removeTimescale: " + ts);
    long currentTime = clock.getCurrentTime();

    Subscription<Timescale> ss = subscriptions.get(ts);
    if (ss == null) {
      LOG.log(Level.WARNING, "Deletion error: Timescale " + ts + " not exists. ");
    } else {
      this.slicedWindow.onTimescaleDeletion(ts);
      this.relationCube.onTimescaleDeletion(ts);
      ss.unsubscribe();
    }
  }

  @Override
  public void close() throws Exception {
    clock.close();
  }
}
