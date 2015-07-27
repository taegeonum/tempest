package edu.snu.tempest.operators.dynamicmts.impl;

import edu.snu.tempest.operators.Timescale;
import edu.snu.tempest.operators.common.Aggregator;
import edu.snu.tempest.operators.common.impl.DefaultOverlappingWindowOperatorImpl;
import edu.snu.tempest.operators.parameters.CachingRate;
import org.apache.reef.tang.annotations.Parameter;
import edu.snu.tempest.operators.common.MTSOperatorScheduler;
import edu.snu.tempest.operators.common.OverlappingWindowOperator;
import edu.snu.tempest.operators.common.Subscription;
import edu.snu.tempest.operators.common.impl.DefaultMTSOperatorSchedulerImpl;
import edu.snu.tempest.operators.dynamicmts.DynamicMTSOperator;
import edu.snu.tempest.operators.dynamicmts.DynamicSlicedWindowOperator;
import edu.snu.tempest.operators.dynamicmts.signal.MTSSignalReceiver;
import edu.snu.tempest.operators.parameters.InitialStartTime;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * DynamicMTSOperatorImpl dynamically adds/removes timescales
 * and produces multi-timescale outputs.
 * @param <I> input
 * @param <V> output
 */
public final class DynamicMTSOperatorImpl<I, V> implements DynamicMTSOperator<I> {
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
  private final DynamicRelationCubeImpl<V> relationCube;

  /**
   * A sliced window operator for mts partial aggregation.
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
   * DynamicMTSOperatorImpl.
   * @param aggregator an aggregator for window aggregation
   * @param timescales an initial timescales
   * @param handler an mts output handler
   * @param receiver a receiver for triggering timescale addition/deletion
   * @param cachingRate cachingRate for cachingRatePolicy
   * @param startTime an initial start time of the operator
   */
  @Inject
  public DynamicMTSOperatorImpl(final Aggregator<I, V> aggregator,
                                final List<Timescale> timescales,
                                final MTSOutputHandler<V> handler,
                                final MTSSignalReceiver receiver,
                                @Parameter(CachingRate.class) final double cachingRate,
                                @Parameter(InitialStartTime.class) final long startTime) {
    this.outputHandler = handler;
    this.relationCube = new DynamicRelationCubeImpl<>(timescales, aggregator,
        new CachingRatePolicy(timescales, cachingRate), startTime);
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
   * Start mts window operation.
   */
  @Override
  public void start() {
    if (started.compareAndSet(false, true)) {
      LOG.log(Level.INFO, "DynamicMTSOperatorImpl start");
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
    LOG.log(Level.INFO, "MTSOperator addTimescale: " + ts);
    //1. add timescale to SlicedWindowOperator
    this.slicedWindow.onTimescaleAddition(ts, startTime);

    //2. add timescale to RelationCube.
    this.relationCube.onTimescaleAddition(ts, startTime);

    //3. add overlapping window operator
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
      this.slicedWindow.onTimescaleDeletion(ts);
      this.relationCube.onTimescaleDeletion(ts);
      ss.unsubscribe();
    }
  }

  @Override
  public void close() throws Exception {
    mtsScheduler.close();
  }
}
