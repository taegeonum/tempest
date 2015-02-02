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
  private long logicalTime = 0;
  private final long tickTime;
  private final AtomicBoolean started = new AtomicBoolean(false);

  @NamedParameter(doc = "clock tick time (ms)", default_value = "1000")
  public static final class TickTime implements Name<Long> {}
  
  public DefaultMTSClockImpl(final SlicedWindowOperator<?> swo,
      final long tickTime,
      final TimeUnit tickTimeUnit, 
      int schedulerThread,
      int fixThread) {
    this.handlers = new ConcurrentLinkedQueue<OverlappingWindowOperator<?>>();
    this.scheduler = Executors.newScheduledThreadPool(schedulerThread, new DefaultThreadFactory("MTSClock")); // FIXME: parameterize the number of threads. 
    this.executor = Executors.newFixedThreadPool(fixThread); // FIXME: parameterize the number of threads. 
    this.slicedWindowOperator = swo;
    this.tickTime = tickTimeUnit.toMillis(tickTime);
  }
  
  @Inject
  public DefaultMTSClockImpl(final SlicedWindowOperator<?> swo,
      @Parameter(TickTime.class) final long tickTime,
      final TimeUnit tickTimeUnit) {
    this.handlers = new ConcurrentLinkedQueue<OverlappingWindowOperator<?>>();
    this.scheduler = Executors.newScheduledThreadPool(10, new DefaultThreadFactory("MTSClock")); // FIXME: parameterize the number of threads. 
    this.executor = Executors.newFixedThreadPool(40); // FIXME: parameterize the number of threads. 
    this.slicedWindowOperator = swo;
    this.tickTime = tickTimeUnit.toMillis(tickTime);
  }
  
  @Inject
  public DefaultMTSClockImpl(final SlicedWindowOperator<?> swo) {
    this(swo, 1L, TimeUnit.SECONDS);
  }

  @Override
  public void start() {

    if (started.compareAndSet(false, true)) {
      LOG.log(Level.INFO, "Clock start");
      
      scheduler.scheduleAtFixedRate(new Runnable() {
        @Override
        public void run() {
          logicalTime += 1;
          final LogicalTime time = new LogicalTime(logicalTime);

          LOG.log(Level.FINE, "Clock tickTime: " + logicalTime);

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
  public LogicalTime getCurrentTime() {
    return new LogicalTime(logicalTime);
  }
  

  @Override
  public void close() {
    scheduler.shutdown();
    executor.shutdown();
  }


}
