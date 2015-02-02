package org.edu.snu.onthefly.operator.impl;

import org.edu.snu.tempest.Timescale;
import org.edu.snu.tempest.operator.MTSOperator.Aggregator;
import org.edu.snu.tempest.operator.MTSOperator.OutputHandler;
import org.edu.snu.tempest.operator.SlicedWindowOperator;
import org.edu.snu.tempest.operator.WindowOutput;
import org.edu.snu.tempest.operator.impl.LogicalTime;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class OTFSlicedWindowOperatorImpl<I, V> implements SlicedWindowOperator<I> {

  private final static Logger LOG = Logger.getLogger(OTFSlicedWindowOperatorImpl.class.getName());
  
  private final Aggregator<I, V> aggregator;
  private final PriorityQueue<SliceInfo> sliceQueue;
  private long prevSliceTime = 0;
  private long nextSliceTime = 0;
  private V innerMap;

  private final OutputHandler<V> outputHandler;
  private final List<OverlappingWindowOperator> overlappingWindowOperators;
  
  private final ExecutorService executor;

  @Inject
  public OTFSlicedWindowOperatorImpl(
      final Aggregator<I, V> aggregator,
      final OutputHandler<V> outputHandler,
      final List<Timescale> timescales) {
    this.aggregator = aggregator;
    this.sliceQueue = new PriorityQueue<SliceInfo>(10, new SliceInfoComparator());
    this.outputHandler = outputHandler;
    this.innerMap = aggregator.init();
    this.overlappingWindowOperators = new LinkedList<>();
    this.executor = Executors.newFixedThreadPool(40);

    initializeWindowState(timescales);
    for (Timescale ts : timescales) {
      this.overlappingWindowOperators.add(new OverlappingWindowOperator(ts));
    }
    nextSliceTime = advanceWindowGetNextSlice();
  }
  
  @Override
  public void onNext(final LogicalTime time) {
    LOG.log(Level.FINE, "SlicedWindow tickTime " + time + ", nextSlice: " + nextSliceTime);

    synchronized (sliceQueue) {
      if (nextSliceTime == time.logicalTime) {
        LOG.log(Level.FINE, "SlicedQueue: " + sliceQueue);
        LOG.log(Level.FINE, "Sliced : [" + prevSliceTime + "-" + time.logicalTime + "]");
        // saves output to OLT
        final V output = innerMap;
        final long elapsed = time.logicalTime - prevSliceTime;
        // send to overlappingwindows
        for (final OverlappingWindowOperator operator : overlappingWindowOperators) {
          executor.submit(new Runnable() {
            @Override
            public void run() {
              operator.aggregate(time, elapsed, output);
            }
          });
        }

        innerMap = aggregator.init();
        prevSliceTime = nextSliceTime;
        nextSliceTime = advanceWindowGetNextSlice();
      }
    }
  }

  @Override
  public void execute(final I val) {
    LOG.log(Level.FINE, "SlicedWindow aggregates input of [" + val + "]");
    innerMap = aggregator.partialAggregate(innerMap, val);
  }

  @Override
  public void onTimescaleAddition(final Timescale ts, final LogicalTime startTime) {
    LOG.log(Level.INFO, "SlicedWindow addTimescale " + ts);
    // Add slices 
    synchronized (sliceQueue) {
      addSlices(startTime.logicalTime, ts);

      long sliceTime = sliceQueue.peek().sliceTime;
      if (sliceTime <= nextSliceTime) {
        nextSliceTime = advanceWindowGetNextSlice();
      }
      
      overlappingWindowOperators.add(new OverlappingWindowOperator(ts));
    }
  }

  @Override
  public void onTimescaleDeletion(final Timescale ts, final LogicalTime startTime) {
    LOG.log(Level.INFO, "SlicedWindow removeTimescale " + ts);
    
    synchronized (sliceQueue) {
      for (Iterator<SliceInfo> iterator = sliceQueue.iterator(); iterator.hasNext();) {
        SliceInfo slice = iterator.next();
        if (slice.timescale.equals(ts)) {
          iterator.remove();
        }
      }
    }
  }

  /*
   * This method is based on "On-the-Fly Sharing " paper.
   * Similar to initializeWindowState function 
   */
  private void initializeWindowState(final List<Timescale> timescales) {
    LOG.log(Level.INFO, "SlicedWindow initialization");

    for (Timescale ts : timescales) {
      addSlices(0, ts);
    }
  }
  
  /*
   * Similar to addEdges function in the "On-the-Fly ... " paper
   */
  private void addSlices(final long startTime, final Timescale ts) {
    long pairedB = ts.windowSize % ts.intervalSize;
    long pairedA = ts.intervalSize - pairedB;
    
    synchronized (sliceQueue) {
      sliceQueue.add(new SliceInfo(startTime + pairedA, ts, false));
      sliceQueue.add(new SliceInfo(startTime + pairedA + pairedB, ts, true));
    }
  }
  
  /*
   * Similar to advanceWindowGetNextEdge function in the "On-the-Fly ..." paper
   */
  private long advanceWindowGetNextSlice() {
    SliceInfo info = null;

    synchronized (sliceQueue) {
      if (sliceQueue.size() == 0) {
        return 0;
      }

      long time = sliceQueue.peek().sliceTime;

      while (time == sliceQueue.peek().sliceTime) {
        info = sliceQueue.poll();
        if (info.last) {
          addSlices(info.sliceTime, info.timescale);
        }
      }
    }
    
    return info.sliceTime;
  }
  
  public void close() {
    executor.shutdown();
  }
  
  private final class SliceInfo {
    
    public final long sliceTime;
    public final Timescale timescale;
    public final boolean last; 
    
    SliceInfo(long sliceTime, Timescale timescale, boolean last) {
      this.sliceTime = sliceTime;
      this.timescale = timescale;
      this.last = last;
    }
  }
  
  private final class SliceInfoComparator implements Comparator<SliceInfo> {

    @Override
    public int compare(SliceInfo o1, SliceInfo o2) {
      if (o1.sliceTime < o2.sliceTime) {
        return -1;
      } else if (o1.sliceTime > o2.sliceTime) {
        return 1;
      } else {
        return 0;
      }
    }
  }
  
  class OverlappingWindowOperator {
    
    final Timescale timescale;
    final Queue<TimeAndOutput> buffer;
    long currentSize = 0;
    long currentInterval = 0;
    
    public OverlappingWindowOperator(Timescale ts) {
      this.timescale = ts;
      this.buffer = new LinkedList<>();
    }

    public synchronized void aggregate(LogicalTime currentTime, long sliceTime, V output) {
      currentSize += sliceTime;
      currentInterval += sliceTime;
      
      buffer.add(new TimeAndOutput(sliceTime, output));
      
      if (currentInterval % timescale.intervalSize == 0) {
        final long aggStartTime = System.nanoTime();
        if (currentSize > timescale.windowSize) {
          long pollTime = 0;
          while(pollTime < timescale.intervalSize) {
            TimeAndOutput o = buffer.poll();
            pollTime += o.sliceTime;
          }
        }
        
        List<V> partials = new LinkedList<>();
        // aggregate 
        int i = 0;
        for (TimeAndOutput partial : buffer) {
          partials.add(partial.output);
        }
        final V finalResult = aggregator.finalAggregate(partials);
        long etime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - aggStartTime);
        outputHandler.onNext(new WindowOutput<V>(this.timescale, finalResult,
            currentTime.logicalTime - timescale.windowSize, currentTime.logicalTime, etime));
      }
    }
    class TimeAndOutput {
      final long sliceTime;
      final V output;
      public TimeAndOutput(final long sliceTime, final V output) {
        this.sliceTime = sliceTime;
        this.output = output;
      }
    }
  }
}


