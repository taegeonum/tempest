package org.edu.snu.tempest.operator.impl;

import org.edu.snu.tempest.Timescale;
import org.edu.snu.tempest.operator.MTSOperator.Aggregator;
import org.edu.snu.tempest.operator.SlicedWindowOperator;
import org.edu.snu.tempest.operator.relationcube.RelationCube;

import javax.inject.Inject;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
 * This implementation is based on "On-the-fly Sharing for Streamed Aggregation" paper
 * It chops input stream into paired sliced window. 
 * 
 */
public final class DefaultSlicedWindowOperatorImpl<I, V> implements SlicedWindowOperator<I> {

  private final static Logger LOG = Logger.getLogger(DefaultSlicedWindowOperatorImpl.class.getName());
  
  private final Aggregator<I, V> aggregator;
  private final RelationCube<V> relationCube;
  private final PriorityQueue<SliceInfo> sliceQueue;
  private long prevSliceTime = 0;
  private long nextSliceTime = 0;
  private V innerMap;
  private final List<Timescale> timescales;
  private final Object sync = new Object();
  
  @Inject
  public DefaultSlicedWindowOperatorImpl(
      final Aggregator<I, V> aggregator,
      final List<Timescale> timescales,
      final RelationCube<V> relationCube) {
    this.aggregator = aggregator;
    this.relationCube = relationCube;
    this.innerMap = aggregator.init();
    this.sliceQueue = new PriorityQueue<SliceInfo>(10, new SliceInfoComparator());
    this.timescales = timescales;

    initializeWindowState();
    nextSliceTime = nextSliceTime();
  }
  
  @Override
  public synchronized void onNext(final LogicalTime time) {
    LOG.log(Level.FINE, "SlicedWindow tickTime " + time + ", nextSlice: " + nextSliceTime);
      if (nextSliceTime == time.logicalTime) {
        LOG.log(Level.FINE, "Sliced : [" + prevSliceTime + "-" + time.logicalTime + "]");
        synchronized(sync) {
          V output = innerMap;
          innerMap = aggregator.init();
          // saves output to RelationCube
          relationCube.savePartialOutput(prevSliceTime, nextSliceTime, output);
        }
        prevSliceTime = nextSliceTime;
        nextSliceTime = nextSliceTime();
      }
  }

  @Override
  public void execute(final I val) {
    LOG.log(Level.FINE, "SlicedWindow aggregates input of [" +  val + "]");
    synchronized (sync) {
      innerMap = aggregator.partialAggregate(innerMap, val);
    }
  }

  private long nextSliceTime() {
    return advanceWindowGetNextSlice();
  }

  @Override
  public void onTimescaleAddition(final Timescale ts, final LogicalTime time) {
    LOG.log(Level.INFO, "SlicedWindow addTimescale " + ts);
    // Add slices
    synchronized (sliceQueue) {
      long nextSliceTime = sliceQueue.peek().sliceTime;
      addSlices(time.logicalTime, ts);

      long sliceTime = sliceQueue.peek().sliceTime;
      while (sliceTime < nextSliceTime) {
        sliceTime = advanceWindowGetNextSlice();
      }
    }

  }

  @Override
  public void onTimescaleDeletion(final Timescale ts, final LogicalTime time) {
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
  private void initializeWindowState() {
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


}
