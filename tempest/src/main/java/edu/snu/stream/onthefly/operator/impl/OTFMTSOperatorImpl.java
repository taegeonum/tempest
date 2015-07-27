package edu.snu.stream.onthefly.operator.impl;

import edu.snu.tempest.operators.Timescale;
import edu.snu.tempest.operators.common.Aggregator;
import edu.snu.tempest.operators.common.MTSOperatorScheduler;
import edu.snu.tempest.operators.common.OverlappingWindowOperator;
import edu.snu.tempest.operators.common.Subscription;
import edu.snu.tempest.operators.common.impl.DefaultMTSOperatorSchedulerImpl;
import edu.snu.tempest.operators.common.impl.DefaultOverlappingWindowOperatorImpl;
import edu.snu.tempest.operators.dynamicmts.DynamicMTSOperator;
import edu.snu.tempest.operators.dynamicmts.DynamicSlicedWindowOperator;
import edu.snu.tempest.operators.dynamicmts.impl.DynamicMTSOperatorImpl;
import edu.snu.tempest.operators.dynamicmts.impl.DynamicSlicedWindowOperatorImpl;
import edu.snu.tempest.operators.dynamicmts.signal.MTSSignalReceiver;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * "On-the-fly sharing for streamed aggregation"'s operator
 * for multi-time scale sliding window operator.
 *
 * This operator is for evaluation.
 */
public final class OTFMTSOperatorImpl<I, V> implements DynamicMTSOperator<I> {
  private static final Logger LOG = Logger.getLogger(DynamicMTSOperatorImpl.class.getName());

  /**
   * Is this window operator started or not.
   */
  private final AtomicBoolean started = new AtomicBoolean(false);

  /**
   * An output handler for multi-time scale window outputs.
   */
  private final MTSOutputHandler<V> outputHandler;

  /**
   * A mts scheduler for mts window operation.
   */
  private final MTSOperatorScheduler mtsScheduler;

  /**
   * A relationCube for saving outputs.
   */
  private final OTFRelationCubeImpl<V> relationCube;

  /**
   * A sliced window operator for partial aggregation.
   */
  private final DynamicSlicedWindowOperator<I> slicedWindow;

  /**
   * OverlappingWindowOperator subscriptions.
   */
  private final Map<Timescale, Subscription<Timescale>> subscriptions;

  /**
   * SignalReceiver for triggering timescale addition/deletion.
   */
  private final MTSSignalReceiver receiver;

  /**
   * "On-the-fly sharing for streamed aggregation"'s operator
   * for multi-time scale sliding window operator.
   * @param aggregator an aggregator for window aggregation.
   * @param timescales an initial timescales for multi-timescale window operation.
   * @param handler an output handler for receiving multi-timescale window outputs.
   * @param receiver a receiver for triggering timescale addition/deletion.
   * @param startTime an initial start time of the operator.
   */
  @Inject
  public OTFMTSOperatorImpl(final Aggregator<I, V> aggregator,
                                final List<Timescale> timescales,
                                final MTSOutputHandler<V> handler,
                                final MTSSignalReceiver receiver,
                                final Long startTime) {
    this.outputHandler = handler;
    this.relationCube = new OTFRelationCubeImpl<>(timescales, aggregator, startTime);
    this.subscriptions = new HashMap<>();
    this.receiver = receiver;
    this.receiver.addTimescaleSignalListener(this);

    this.slicedWindow = new DynamicSlicedWindowOperatorImpl<>(aggregator, timescales,
        relationCube, startTime);
    this.mtsScheduler = new DefaultMTSOperatorSchedulerImpl(slicedWindow);

    for (final Timescale timescale : timescales) {
      final OverlappingWindowOperator<V> owo = new DefaultOverlappingWindowOperatorImpl<V>(
          timescale, relationCube, outputHandler, startTime);
      final Subscription<Timescale> ss = mtsScheduler.subscribe(owo);
      subscriptions.put(ss.getToken(), ss);
    }
  }

  /**
   * Start window operation.
   */
  @Override
  public void start() {
    if (started.compareAndSet(false, true)) {
      LOG.log(Level.INFO, "MTSOperator start");
      this.mtsScheduler.start();
      try {
        this.receiver.start();
      } catch (final Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Aggregate input and produce partially aggregated outputs.
   * @param val input value
   */
  @Override
  public void execute(final I val) {
    LOG.log(Level.FINEST, "MTSOperator execute : ( " + val + ")");
    this.slicedWindow.execute(val);
  }

  /**
   * Add a timescale dynamically and produce outputs for this timescale.
   * @param ts timescale to be added.
   * @param startTime the time when timescale is added. This is used for synchronization.
   */
  @Override
  public synchronized void onTimescaleAddition(final Timescale ts, final long startTime) {
    //1. add timescale to SlicedWindowOperator
    this.slicedWindow.onTimescaleAddition(ts, startTime);

    //2. add timescale to RelationCube.
    this.relationCube.onTimescaleAddition(ts, startTime);

    //3. add overlapping window operator
    LOG.log(Level.INFO, "MTSOperator addTimescale: " + ts);
    final OverlappingWindowOperator<V> owo = new DefaultOverlappingWindowOperatorImpl<>(
        ts, relationCube, outputHandler, startTime);
    final Subscription<Timescale> ss = this.mtsScheduler.subscribe(owo);
    subscriptions.put(ss.getToken(), ss);
  }

  /**
   * Remove a timescale dynamically.
   * @param ts timescale to be deleted.
   */
  @Override
  public synchronized void onTimescaleDeletion(final Timescale ts) {
    LOG.log(Level.INFO, "MTSOperator removeTimescale: " + ts);
    final Subscription<Timescale> ss = subscriptions.get(ts);
    if (ss == null) {
      LOG.log(Level.WARNING, "Deletion error: Timescale " + ts + " not exists. ");
    } else {
      ss.unsubscribe();
      this.relationCube.onTimescaleDeletion(ts);
      this.slicedWindow.onTimescaleDeletion(ts);
    }
  }

  @Override
  public void close() throws Exception {
    mtsScheduler.close();
  }
}
