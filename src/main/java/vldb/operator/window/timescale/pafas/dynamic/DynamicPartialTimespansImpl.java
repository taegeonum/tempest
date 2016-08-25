/*
 * Copyright (C) 2015 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package vldb.operator.window.timescale.pafas.dynamic;

import org.apache.reef.tang.annotations.Parameter;
import vldb.operator.window.timescale.TimeMonitor;
import vldb.operator.window.timescale.Timescale;
import vldb.operator.window.timescale.pafas.Node;
import vldb.operator.window.timescale.parameter.StartTime;

import javax.inject.Inject;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/** This implementation is based on "On-the-fly Sharing for Streamed Aggregation" paper.
 * Reference: S. Krishnamurthy, C. Wu, and M. Franklin. On-the-fly sharing
 * for streamed aggregation. In ACM SIGMOD, 2006
 * It returns next slice time for slicing input stream into paired sliced window.
 */
public final class DynamicPartialTimespansImpl<T> implements DynamicPartialTimespans<T> {
  private static final Logger LOG = Logger.getLogger(DynamicPartialTimespansImpl.class.getName());

  /**
   * The list of timescale.
   */
  private final List<Timescale> timescales;

  /**
   * A start time.
   */
  private final long startTime;

  /**
   * Map of start_time and next timespan map.
   */
  private final Map<Long, Node<T>> partialTimespanMap;

  private long currIndex;
  private long rebuildSize;

  private final WindowManager windowManager;

  private final TimeMonitor timeMonitor;

  /**
   * StaticComputationReuserImpl Implementation.
   * @param startTime an initial start time
   */
  @Inject
  private DynamicPartialTimespansImpl(final WindowManager windowManager,
                                      @Parameter(StartTime.class) final long startTime,
                                      final TimeMonitor timeMonitor) {
    LOG.info("START " + this.getClass());
    this.timescales = windowManager.timescales;
    this.timeMonitor = timeMonitor;
    this.startTime = startTime;
    this.partialTimespanMap = new HashMap<>();
    this.rebuildSize = windowManager.getRebuildSize();
    this.currIndex = startTime + rebuildSize;
    this.windowManager = windowManager;
    createSliceQueue();
    //System.out.println("CRAET: " + partialTimespanMap);
  }

  /**
   * It creates the list of next slice time.
   * This method is based on "On-the-Fly Sharing for Streamed Aggregation" paper.
   * Similar to initializeWindowState function
   */
  private void createSliceQueue() {
    // add sliced window edges
    final List<Long> sliceQueue = new LinkedList<>();
    for (final Timescale ts : timescales) {
      final long pairedB = ts.windowSize % ts.intervalSize;
      final long pairedA = ts.intervalSize - pairedB;
      long time = pairedA;
      boolean odd = true;

      while (time <= rebuildSize) {
        sliceQueue.add(startTime + time);
        if (odd) {
          time += pairedB;
        } else {
          time += pairedA;
        }
        odd = !odd;
      }
    }
    Collections.sort(sliceQueue);
    long prevSliceTime = startTime;
    for (final long nextSliceTime : sliceQueue) {
      if (prevSliceTime != nextSliceTime) {
        //System.out.println("Add: " + prevSliceTime + ", " + nextSliceTime);
        partialTimespanMap.put(prevSliceTime, new Node<T>(prevSliceTime, nextSliceTime, true));
        prevSliceTime = nextSliceTime;
      }
    }
    //System.out.println(sliceQueue);
    //System.out.println(sliceQueue);
    //System.out.println(sliceQueue2);
    //System.out.println(partialTimespanMap);
    LOG.log(Level.FINE, "Sliced queue: " + partialTimespanMap);
  }

  @Override
  public Node<T> getNextPartialTimespanNode(final long currTime) {
    //System.out.println("currTime: " + currTime);
    //System.out.println("GET_NEXT_PARTIAL: " + adjStartTime(currTime) +", " + partialTimespanMap.get(adjStartTime(currTime)));
    return partialTimespanMap.get(currTime);
  }

  private void buildSlice(final long start, final long end) {
    long nextStep = end;
    //System.out.println("partial build: " + currIndex + ", "+ nextStep);
    //System.out.println("adjTime null:" + currTime + ", " + adjTime);
    final List<Long> sliceQueue = new LinkedList<>();
    for (final Timescale ts : timescales) {
      final long tsStartTime = windowManager.timescaleStartTime(ts);
      long adjStartTime = start - (start - tsStartTime) % ts.intervalSize;
      boolean odd = false;
      while (adjStartTime <= nextStep) {
        final long pairedB = ts.windowSize % ts.intervalSize;
        final long pairedA = ts.intervalSize - pairedB;
        if (adjStartTime >= tsStartTime) {
          sliceQueue.add(adjStartTime);
        }
        if (odd) {
          adjStartTime += pairedB;
        } else {
          adjStartTime += pairedA;
        }
        odd = !odd;
      }
    }
    Collections.sort(sliceQueue);
    //System.out.println(sliceQueue + ", " + timescales);
    long prevSliceTime = sliceQueue.get(0);
    for (final long nextSliceTime : sliceQueue) {
      if (prevSliceTime != nextSliceTime) {
        if (partialTimespanMap.get(prevSliceTime) == null) {
          //System.out.println("Add: " + prevSliceTime + ", " + nextSliceTime);
          partialTimespanMap.put(prevSliceTime, new Node<T>(prevSliceTime, nextSliceTime, true));
          //System.out.println("PUT " + prevSliceTime + ": " + partialTimespanMap.get(prevSliceTime));
        }
        prevSliceTime = nextSliceTime;
      }
    }
    currIndex = nextStep;
  }

  @Override
  public long getNextSliceTime(final long currTime) {
    // TODO: Incremental build
    //System.out.println(partialTimespanMap);
    //System.out.println("currTiume : " + currTime + ", currIndex: " + currIndex + ", rebuildSize: " + rebuildSize);
    if (currTime + rebuildSize > currIndex) {
      final long s = System.nanoTime();
      buildSlice(currIndex, currTime + rebuildSize);
      final long e = System.nanoTime();
      timeMonitor.continuousTime += (e-s);
    }
    Node<T> node = partialTimespanMap.get(currTime);
    if (node == null) {
      // Find next partial
      for (long i = currTime; i <= currIndex; i++) {
        node = partialTimespanMap.get(i);
        if (node != null) {
          break;
        }
      }
    }
    return node.end;
  }

  @Override
  public void addWindow(final Timescale window, final long prevSliceTime, final long time) {
    // Remove all partial
    //System.out.println("window " + window + " Remove : " + prevSliceTime + ", " + currIndex);
    for (long removeIndex = prevSliceTime; removeIndex <= currIndex; removeIndex += 1) {
      partialTimespanMap.remove(removeIndex);
    }

    // Create new partials
    rebuildSize = windowManager.getRebuildSize();
    //System.out.println("Rebuild : " + prevSliceTime + ", " + (time + rebuildSize));
    buildSlice(prevSliceTime, time + rebuildSize);
    //System.out.println("Rebuild " + partialTimespanMap);
  }

  @Override
  public void removeWindow(final Timescale window, final long prevSliceTime, final long time, final long windowStart) {
    // Remove all partials
    //System.out.println("Prev rebuild: " + partialTimespanMap);
    for (long removeIndex = time+1; removeIndex <= currIndex; removeIndex += 1) {
      partialTimespanMap.remove(removeIndex);
    }
    //System.out.println("Removed rebuild: " + partialTimespanMap);

    // Create new partials
    rebuildSize = windowManager.getRebuildSize();
    buildSlice(time+1, time + rebuildSize);

    // Adjust that 6-12  18-20, the 12-18 can be empty
    // [----------------------]
    // prevSliceTime                    [---------]
    //                     ^
    //                   change
    final Node<T> lastSliceNode = partialTimespanMap.get(prevSliceTime);
    if (partialTimespanMap.get(lastSliceNode.end) == null) {
      // find next slice
      long nextS = lastSliceNode.end;
      for (long s = nextS+1; s <= time + rebuildSize; s++) {
        if (partialTimespanMap.get(s) != null) {
          nextS = s;
          break;
        }
      }
      partialTimespanMap.remove(prevSliceTime);
      partialTimespanMap.put(prevSliceTime, new Node<T>(prevSliceTime, nextS, true));
    }
    //System.out.println("After rebuild: " + partialTimespanMap);
  }

  @Override
  public void removeNode(final long startTime) {
    partialTimespanMap.remove(startTime);
  }

  @Override
  public String toString() {
    return partialTimespanMap.toString();
  }
}