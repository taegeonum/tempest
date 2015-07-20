package org.edu.snu.tempest.operator.relationcube.impl;

import org.edu.snu.tempest.Timescale;
import org.edu.snu.tempest.operator.MTSOperator;
import org.edu.snu.tempest.operator.impl.NotFoundException;
import org.edu.snu.tempest.operator.impl.TimeAndValue;
import org.edu.snu.tempest.operator.relationcube.OutputLookupTable;
import org.edu.snu.tempest.operator.relationcube.RelationCube;

import javax.inject.Inject;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

public class StaticRelationCubeImpl<T> implements RelationCube<T> {
  private static final Logger LOG = Logger.getLogger(StaticRelationCubeImpl.class.getName());

  private final OutputLookupTable<Node> table;
  private final List<Timescale> timescales;
  private final MTSOperator.Aggregator<?, T> finalAggregator;
  private final List<Long> sliceQueue;
  private final long period;
  private int index = 0;


  @Inject
  public StaticRelationCubeImpl(final List<Timescale> timescales,
                                final MTSOperator.Aggregator<?, T> finalAggregator) {
    this.table = new DefaultOutputLookupTableImpl<>();
    this.timescales = timescales;
    this.finalAggregator = finalAggregator;
    this.sliceQueue = new LinkedList<>();
    this.period = calculatePeriod(timescales);
    LOG.log(Level.INFO, StaticRelationCubeImpl.class + " started. PERIOD: " + period);

    // create RelationGraph.
    addSlicedWindowNodeAndEdge();
    addOverlappingWindowNodeAndEdge();
  }

  @Override
  public void savePartialOutput(long startTime, long endTime, T output) {

    long start = adjStartTime(startTime);
    long end = adjEndTime(endTime);

    if (start >= end) {
      start -= period;
    }
    LOG.log(Level.INFO, "savePartialOutput: " + startTime + " - " + endTime +", " + start  + " - " + end);

    Node node = null;
    try {
      node = table.lookup(start, end);
    } catch (NotFoundException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    synchronized (node) {
      if (node.getRefCnt() != 0) {
        node.setState(output);
        // set current end time
        node.setCurrentStartTime(startTime);
        node.setCurrentEndTime(endTime);
        node.notifyAll();
      }
    }
  }

  @Override
  public T finalAggregate(long startTime, long endTime, Timescale ts) {
    LOG.log(Level.FINE, "Lookup " + startTime + ", " + endTime);

    long start = adjStartTime(startTime);
    long end = adjEndTime(endTime);

    // reuse!
    if (start >= end) {
      start -= period;
    }

    Node node = null;
    try {
      node = table.lookup(start, end);
    } catch (NotFoundException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    List<T> list = new LinkedList<>();
    for (Node n : node.getDependencies()) {
      synchronized (n) {
        long currentNodeStartTime = n.getCurrentStartTime();
        long currentNodeEndTime = n.getCurrentEndTime();

        if (!(currentNodeStartTime >= startTime && currentNodeEndTime <= endTime)) {
          if (startTime < 0 && n.start > endTime) {
            // skip
          } else {
            // wait until dependent output are generated.
            LOG.log(Level.INFO, "finalAggregate sleep: " + currentNodeStartTime + " - "
                + currentNodeEndTime + ", " + startTime + " - " + endTime);
            try {
              n.wait();
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }
        }

        if (n.getState() != null) {
          list.add(n.getState());
          n.decreaseRefCnt();
        }
      }
    }

    T output = finalAggregator.finalAggregate(list);
    synchronized (node) {
      if (node.getRefCnt() != 0) {
        node.setState(output);
        // set current end time
        node.setCurrentStartTime(startTime);
        node.setCurrentEndTime(endTime);
        node.notifyAll();
      }
    }

    LOG.log(Level.FINE, "finalAggregate: " + startTime + " - " + endTime + ", " + start + " - " + end
        + ", period: " + period + ", dep: " + node.getDependencies().size() + ", listLen: " + list.size());
    return output;
  }

  @Override
  public void addTimescale(Timescale ts, long time) {
    // do nothing
  }

  @Override
  public void removeTimescale(Timescale ts, long time) {
    // do nothing
  }

  private long adjStartTime(long time) {
    long adj = time % period;

    LOG.log(Level.FINE, "time: " + time + ", adj: " + adj);

    return adj;
  }

  private long adjEndTime(long time) {
    long adj = time % period == 0 ? period : time % period;
    LOG.log(Level.FINE, "time: " + time + ", adj: " + adj);

    return adj;
  }

  /*
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
        table.saveOutput(startTime, endTime, new Node(startTime, endTime));
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
        Node referer = new Node(start, time);
        List<Node> dependencies = new LinkedList<>();

        // lookup dependencies
        long st = start;
        while (st < time) {
          TimeAndValue<Node> elem;
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

        for (Node elem : dependencies) {
          referer.addDependency(elem);
        }

        table.saveOutput(start, time, referer);

        LOG.log(Level.FINE, "(" + start + ", " + time + ") dependencies2: " + dependencies);
      }
    }
  }

  /*
  * Find period of repeated pattern
  * period = c * lcm ( i_{1}, i_{2}, ..., i_{k} ) ( i_{k} is interval of k-th timescale)
  * c is natural number which satisfies period >= largest_window_size
  */
  public static final long calculatePeriod(List<Timescale> timescales) {
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

  public class Node {

    private final List<Node> dependencies;
    private int refCnt;
    private int initialRefCnt;
    private T state;
    public final long start;
    public final long end;
    private AtomicLong currentEndTime;
    private AtomicLong currentStartTime;

    public Node(long start, long end) {
      this.dependencies = new LinkedList<>();
      refCnt = 0;
      this.start = start;
      this.end = end;
      this.currentStartTime = new AtomicLong(-1);
      this.currentEndTime = new AtomicLong(-1);
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

    public long getCurrentStartTime() {
      return currentStartTime.get();
    }
    public long getCurrentEndTime() {
      return currentEndTime.get();
    }

    public void setCurrentStartTime(long startTime) {
      this.currentStartTime.set(startTime);
    }

    public void setCurrentEndTime(long endTime) {
      this.currentEndTime.set(endTime);
    }

    public void addDependency(Node n) {
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

    public List<Node> getDependencies() {
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
