package org.edu.snu.tempest.operators.dynamicmts.impl;

import org.edu.snu.tempest.operators.Timescale;
import org.edu.snu.tempest.operators.common.Aggregator;
import org.edu.snu.tempest.operators.dynamicmts.DynamicRelationCube;
import org.edu.snu.tempest.operators.dynamicmts.DynamicSlicedWindowOperator;

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
public final class DynamicSlicedWindowOperatorImpl<I, V> implements DynamicSlicedWindowOperator<I> {

  private static final Logger LOG = Logger.getLogger(DynamicSlicedWindowOperatorImpl.class.getName());
  
  private final Aggregator<I, V> aggregator;
  private final DynamicRelationCube<V> relationCube;
  private final PriorityQueue<SliceInfo> sliceQueue;
  private long prevSliceTime = 0;
  private long nextSliceTime = 0;
  private V innerMap;
  private final List<Timescale> timescales;
  private final Object sync = new Object();
  
  @Inject
  public DynamicSlicedWindowOperatorImpl(
      final Aggregator<I, V> aggregator,
      final List<Timescale> timescales,
      final DynamicRelationCube<V> relationCube,
      final Long startTime) {
    this.aggregator = aggregator;
    this.relationCube = relationCube;
    this.innerMap = aggregator.init();
    this.sliceQueue = new PriorityQueue<SliceInfo>(10, new SliceInfoComparator());
    this.timescales = timescales;

    initializeWindowState(startTime);
    nextSliceTime = nextSliceTime();
  }
  
  @Override
  public synchronized void onNext(final Long currTime) {
    LOG.log(Level.FINE, "SlicedWindow tickTime " + currTime + ", nextSlice: " + nextSliceTime);
    while (nextSliceTime < currTime) {
      prevSliceTime = nextSliceTime;
      nextSliceTime = nextSliceTime();
    }

    if (nextSliceTime == currTime) {
      LOG.log(Level.FINE, "Sliced : [" + prevSliceTime + "-" + currTime + "]");
      synchronized (sync) {
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

  @Override
  public void onTimescaleAddition(final Timescale ts, final long startTime) {
    LOG.log(Level.INFO, "SlicedWindow addTimescale " + ts);
    // Add slices
    synchronized (sliceQueue) {
      long nst = sliceQueue.peek().sliceTime;
      addSlices(startTime, ts);
      long sliceTime = sliceQueue.peek().sliceTime;
      while (sliceTime < nst) {
        sliceTime = advanceWindowGetNextSlice();
      }
    }
  }

  @Override
  public void onTimescaleDeletion(final Timescale ts) {
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

  private long nextSliceTime() {
    return advanceWindowGetNextSlice();
  }

  /*
   * This method is based on "On-the-Fly Sharing " paper.
   * Similar to initializeWindowState function
   */
  private void initializeWindowState(final long startTime) {
    LOG.log(Level.INFO, "SlicedWindow initialization");

    for (Timescale ts : timescales) {
      addSlices(startTime, ts);
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
