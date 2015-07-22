package org.edu.snu.tempest.operator.impl;

import org.edu.snu.tempest.Timescale;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.impl.DefaultThreadFactory;
import org.edu.snu.tempest.operator.Clock;
import org.edu.snu.tempest.operator.OverlappingWindowOperator;
import org.edu.snu.tempest.operator.SlicedWindowOperator;
import org.edu.snu.tempest.operator.Subscription;

import javax.inject.Inject;
import java.util.Collection;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class DefaultMTSClockImpl implements Clock {

  private static final Logger LOG = Logger.getLogger(DefaultMTSClockImpl.class.getName());
  
  private final Collection<OverlappingWindowOperator<?>> handlers;
  private final SlicedWindowOperator<?> slicedWindowOperator;
  private final ScheduledExecutorService scheduler;
  private final ExecutorService executor;
  private final long tickTime;
  private final AtomicBoolean started = new AtomicBoolean(false);
  private long prevTime;

  @NamedParameter(doc = "clock tick time (ms)", default_value = "200")
  public static final class TickTime implements Name<Long> {}
  
  public DefaultMTSClockImpl(final SlicedWindowOperator<?> swo,
      final long tickTime,
      final TimeUnit tickTimeUnit, 
      int schedulerThread,
      int fixThread) {
    this.handlers = new ConcurrentLinkedQueue<OverlappingWindowOperator<?>>();
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
    this.handlers = new ConcurrentLinkedQueue<OverlappingWindowOperator<?>>();
    // FIXME: parameterize the number of threads.
    this.scheduler = Executors.newScheduledThreadPool(1,
        new DefaultThreadFactory("MTSClock"));
    // FIXME: parameterize the number of threads.
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
            if (slicedWindowOperator != null) {
              slicedWindowOperator.onNext(time);
            }

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
      }, tickTime, tickTime, TimeUnit.MILLISECONDS);
    }
  }
  
  @Override
  public Subscription<Timescale> subscribe(final OverlappingWindowOperator<?> o) {
    LOG.log(Level.INFO, "Clock subscribe OverlappingWindowOperator: " + o);
    handlers.add(o);
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


}
