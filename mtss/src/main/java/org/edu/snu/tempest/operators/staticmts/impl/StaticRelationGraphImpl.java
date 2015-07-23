package org.edu.snu.tempest.operators.staticmts.impl;

import org.edu.snu.tempest.operators.Timescale;
import org.edu.snu.tempest.operators.common.Aggregator;
import org.edu.snu.tempest.operators.common.NotFoundException;
import org.edu.snu.tempest.operators.common.OutputLookupTable;
import org.edu.snu.tempest.operators.common.impl.TimeAndValue;
import org.edu.snu.tempest.operators.common.impl.DefaultOutputLookupTableImpl;
import org.edu.snu.tempest.operators.staticmts.StaticRelationGraph;

import javax.inject.Inject;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * StaticRelation Graph Implementation.
 *
 * It constructs RelationGraph at start time.
 */
public final class StaticRelationGraphImpl<T> implements StaticRelationGraph<T> {
  private static final Logger LOG = Logger.getLogger(StaticRelationGraphImpl.class.getName());

  private final OutputLookupTable<RelationGraphNode> table;
  private final List<Timescale> timescales;
  private final Aggregator<?, T> finalAggregator;
  private final List<Long> sliceQueue;
  private final long period;
  private int index = 0;
  private final long initialStartTime;

  @Inject
  public StaticRelationGraphImpl(final List<Timescale> timescales,
                                 final Aggregator<?, T> finalAggregator,
                                 final long startTime) {
    this.table = new DefaultOutputLookupTableImpl<>();
    this.timescales = timescales;
    this.finalAggregator = finalAggregator;
    this.sliceQueue = new LinkedList<>();
    this.period = calculatePeriod(timescales);
    this.initialStartTime = startTime;
    LOG.log(Level.INFO, StaticRelationGraphImpl.class + " started. PERIOD: " + period);

    // create RelationGraph.
    addSlicedWindowNodeAndEdge();
    addOverlappingWindowNodeAndEdge();
  }

  @Override
  public void savePartialOutput(final long startTime,
                                final long endTime,
                                final T output) {
    long start = adjStartTime(startTime);
    long end = adjEndTime(endTime);

    if (start >= end) {
      start -= period;
    }
    LOG.log(Level.FINE,
        "savePartialOutput: " + startTime + " - " + endTime +", " + start  + " - " + end);

    RelationGraphNode node = null;
    try {
      node = table.lookup(start, end);
    } catch (NotFoundException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    // save partial output to node
    node.setState(output);
  }

  @Override
  public synchronized long nextSliceTime() {
    return initialStartTime + (index / sliceQueue.size()) * period
        + sliceQueue.get((index++) % sliceQueue.size());
  }

  @Override
  public T finalAggregate(long startTime, long endTime, final Timescale ts) {
    LOG.log(Level.FINE, "Lookup " + startTime + ", " + endTime);

    long start = adjStartTime(startTime);
    long end = adjEndTime(endTime);

    // reuse!
    if (start >= end) {
      start -= period;
    }

    RelationGraphNode node = null;
    try {
      node = table.lookup(start, end);
    } catch (NotFoundException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    List<T> list = new LinkedList<>();
    for (RelationGraphNode child : node.getDependencies()) {
      if (child.getState() != null) {
        list.add(child.getState());
        child.decreaseRefCnt();
      }
    }

    T output = finalAggregator.finalAggregate(list);

    // save the output
    if (node.getRefCnt() != 0) {
      node.setState(output);
    }

    LOG.log(Level.FINE, "finalAggregate: " + startTime + " - " + endTime + ", " + start + " - " + end
        + ", period: " + period + ", dep: " + node.getDependencies().size() + ", listLen: " + list.size());
    return output;
  }

  private long adjStartTime(long time) {
    long adj = (time - initialStartTime) % period;

    LOG.log(Level.INFO, "time: " + time + ", adj: " + adj);

    return adj;
  }

  private long adjEndTime(long time) {
    long adj = (time - initialStartTime) % period == 0 ? period : (time - initialStartTime) % period;
    LOG.log(Level.INFO, "time: " + time + ", adj: " + adj);

    return adj;
  }

  /**
   * This method is based on "On-the-Fly Sharing " paper.
   * Similar to initializeWindowState function
   */
  private void addSlicedWindowNodeAndEdge() {

    // add sliced window edges
    for (Timescale ts : timescales) {
      long pairedB = ts.windowSize % ts.intervalSize;
      long pairedA = ts.intervalSize - pairedB;

      long time = pairedA;
      boolean odd = true;

      while(time <= period) {
        sliceQueue.add(time);

        if (odd) {
          time += pairedB;
        } else {
          time += pairedA;
        }

        odd = !odd;
      }
    }

    Collections.sort(sliceQueue);
    long startTime = 0;
    for (long endTime : sliceQueue) {
      if (startTime != endTime) {
        table.saveOutput(startTime, endTime, new RelationGraphNode(startTime, endTime));
        startTime = endTime;
      }
    }

    LOG.log(Level.FINE, "Sliced queue: " + sliceQueue);
  }

  private void addOverlappingWindowNodeAndEdge() {
    for (Timescale timescale : timescales) {
      for (long time = timescale.intervalSize; time <= period; time += timescale.intervalSize) {

        // create vertex and add it to the table cell of (time, windowsize)
        final long start = time - timescale.windowSize;
        RelationGraphNode referer = new RelationGraphNode(start, time);
        List<RelationGraphNode> dependencies = new LinkedList<>();

        // lookup dependencies
        long st = start;
        while (st < time) {
          TimeAndValue<RelationGraphNode> elem;
          try {
            elem = table.lookupLargestSizeOutput(st, time);
            if (st == elem.endTime) {
              break;
            } else {
              dependencies.add(elem.value);
              //referer.addDependency(elem.value);
              st = elem.endTime;
            }
          } catch (NotFoundException e) {
            try {
              elem = table.lookupLargestSizeOutput(st + period, period);
              dependencies.add(elem.value);
              st = elem.endTime - period;
            } catch (NotFoundException e1) {
              e1.printStackTrace();
              throw new RuntimeException(e1);
            }
          }
        }

        LOG.log(Level.FINE, "(" + start + ", " + time + ") dependencies1: " + dependencies);

        for (RelationGraphNode elem : dependencies) {
          referer.addDependency(elem);
        }

        table.saveOutput(start, time, referer);

        LOG.log(Level.FINE, "(" + start + ", " + time + ") dependencies2: " + dependencies);
      }
    }
  }

  /**
   * Find period of repeated pattern.
   * period = c * lcm ( i_{1}, i_{2}, ..., i_{k} ) ( i_{k} is interval of k-th timescale)
   * c is natural number which satisfies period >= largest_window_size
   */
  public static long calculatePeriod(final List<Timescale> timescales) {
    long period = 0;
    long largestWindowSize = 0;

    for (Timescale ts : timescales) {
      if (period == 0) {
        period = ts.intervalSize;
      } else {
        period = lcm(period, ts.intervalSize);
      }

      // find largest window size
      if (largestWindowSize < ts.windowSize) {
        largestWindowSize = ts.windowSize;
      }
    }

    if (period < largestWindowSize) {
      long div = largestWindowSize / period;

      if (largestWindowSize % period == 0) {
        period *= div;
      } else {
        period *= (div+1);
      }
    }

    return period;
  }

  private static long gcd(long a, long b) {
    while (b > 0) {
      long temp = b;
      b = a % b; // % is remainder
      a = temp;
    }
    return a;
  }

  private static long lcm(long a, long b) {
    return a * (b / gcd(a, b));
  }

  public class RelationGraphNode {

    private final List<RelationGraphNode> dependencies;
    private int refCnt;
    private int initialRefCnt;
    private T state;
    public final long start;
    public final long end;

    public RelationGraphNode(long start, long end) {
      this.dependencies = new LinkedList<>();
      refCnt = 0;
      this.start = start;
      this.end = end;
    }

    public synchronized void decreaseRefCnt() {
      if (refCnt > 0) {
        refCnt--;

        if (refCnt == 0) {
          // Remove state

          LOG.log(Level.FINE, "Release: [" + start + "-" + end + "]");
          state = null;
          refCnt = initialRefCnt;
        }
      }
    }

    public void addDependency(RelationGraphNode n) {
      if (n == null) {
        throw new NullPointerException();
      }
      dependencies.add(n);
      n.increaseRefCnt();
    }

    private void increaseRefCnt() {
      refCnt++;
      initialRefCnt = refCnt;
    }

    public int getRefCnt() {
      return refCnt;
    }

    public List<RelationGraphNode> getDependencies() {
      return dependencies;
    }

    public String toString() {
      boolean isState = !(state == null);
      return "(refCnt: " + refCnt + ", range: [" + start + "-" + end + "]" + ", s: " + isState + ")";
    }

    public T getState() {
      return state;
    }

    public void setState(T state) {
      this.state = state;
    }
  }
}
