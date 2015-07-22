package org.edu.snu.tempest.operators.common.impl;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.impl.DefaultThreadFactory;
import org.edu.snu.tempest.operators.Timescale;
import org.edu.snu.tempest.operators.common.Clock;
import org.edu.snu.tempest.operators.common.OverlappingWindowOperator;
import org.edu.snu.tempest.operators.common.Subscription;
import org.edu.snu.tempest.operators.staticmts.SlicedWindowOperator;

import javax.inject.Inject;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Default MTS Clock implementation.
 *
 * It first executes a SlicedWindowOperator.
 * After that, it executes OverlappingWindowOperators according to the window size.
 */
public final class DefaultMTSClockImpl implements Clock {

  private static final Logger LOG = Logger.getLogger(DefaultMTSClockImpl.class.getName());

  /**
   * Overlapping window operators.
   */
  private final PriorityQueue<OverlappingWindowOperator<?>> handlers;

  /**
   * Sliced window operator.
   */
  private final SlicedWindowOperator<?> slicedWindowOperator;

  /**
   * Scheduler for sliced window operator.
   */
  private final ScheduledExecutorService scheduler;

  /**
   * Executor for overlapping window operator.
   */
  private final ExecutorService executor;

  /**
   * Clock tick time.
   */
  private final long tickTime;

  /**
   * started flag.
   */
  private final AtomicBoolean started = new AtomicBoolean(false);

  /**
   * Previous clock time.
   */
  private long prevTime;

  @NamedParameter(doc = "clock tick time (ms)", default_value = "200")
  public static final class TickTime implements Name<Long> {}
  
  public DefaultMTSClockImpl(final SlicedWindowOperator<?> swo,
      final long tickTime,
      final TimeUnit tickTimeUnit, 
      int schedulerThread,
      int fixThread) {
    this.handlers = new PriorityQueue<>(10, new OWOComparator());
    this.scheduler = Executors.newScheduledThreadPool(schedulerThread,
        new DefaultThreadFactory("MTSClock"));
    this.executor = Executors.newFixedThreadPool(fixThread);
    this.slicedWindowOperator = swo;
    this.tickTime = tickTimeUnit.toMillis(tickTime);
    this.prevTime = 0;
  }
  
  @Inject
  public DefaultMTSClockImpl(final SlicedWindowOperator<?> swo,
      @Parameter(TickTime.class) final long tickTime,
      final TimeUnit tickTimeUnit) {
    this.handlers = new PriorityQueue<>(10, new OWOComparator());
    this.scheduler = Executors.newScheduledThreadPool(1,
        new DefaultThreadFactory("MTSClock"));
    this.executor = Executors.newFixedThreadPool(1);
    this.slicedWindowOperator = swo;
    this.tickTime = tickTimeUnit.toMillis(tickTime);
  }
  
  @Inject
  public DefaultMTSClockImpl(final SlicedWindowOperator<?> swo) {
    this(swo, 200L, TimeUnit.MILLISECONDS);
  }

  @Override
  public void start() {
    if (started.compareAndSet(false, true)) {
      LOG.log(Level.INFO, "Clock start");
      scheduler.scheduleAtFixedRate(new Runnable() {
        @Override
        public void run() {
          final long time = getCurrentTime();
          if (prevTime < time) {
            LOG.log(Level.FINE, "Clock tickTime: " + time);
            prevTime = time;

            // trigger slicedWindowOperator to slice data.
            if (slicedWindowOperator != null) {
              slicedWindowOperator.onNext(time);
            }

            // trigger overlapping window operator to do final aggregation.
            synchronized (handlers) {
              for (final OverlappingWindowOperator<?> handler : handlers) {
                executor.submit(new Runnable() {
                  @Override
                  public void run() {
                    handler.onNext(time);
                  }
                });
              }
            }
          }
        }
      }, tickTime, tickTime, TimeUnit.MILLISECONDS);
    }
  }
  
  @Override
  public Subscription<Timescale> subscribe(final OverlappingWindowOperator<?> o) {
    LOG.log(Level.INFO, "Clock subscribe OverlappingWindowOperator: " + o);
    synchronized (handlers) {
      handlers.add(o);
    }
    return new DefaultSubscription<OverlappingWindowOperator<?>, Timescale>(handlers, o, o.getTimescale());
  }

  @Override
  public long getCurrentTime() {
    return TimeUnit.NANOSECONDS.toSeconds(System.nanoTime());
  }


  @Override
  public void close() {
    scheduler.shutdown();
    executor.shutdown();
  }

  /**
   * An overlapping window operator which has small size of window
   * is executed before another OWOs having large size of window.
   */
  public class OWOComparator implements Comparator<OverlappingWindowOperator> {
    @Override
    public int compare(OverlappingWindowOperator x, OverlappingWindowOperator y) {
      if (x.getTimescale().windowSize < y.getTimescale().windowSize) {
        return -1;
      } else if (x.getTimescale().windowSize > y.getTimescale().windowSize) {
        return 1;
      } else {
        return 0;
      }
    }
  }
}
