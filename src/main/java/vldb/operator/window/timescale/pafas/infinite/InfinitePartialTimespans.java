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
package vldb.operator.window.timescale.pafas.infinite;

import org.apache.reef.tang.annotations.Parameter;
import vldb.operator.window.timescale.Timescale;
import vldb.operator.window.timescale.common.TimescaleParser;
import vldb.operator.window.timescale.pafas.Node;
import vldb.operator.window.timescale.pafas.PartialTimespans;
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
public final class InfinitePartialTimespans<T> implements PartialTimespans {
  private static final Logger LOG = Logger.getLogger(InfinitePartialTimespans.class.getName());

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

  private final long incrementalStep = 3;
  private final long largestWindowSize;
  private long currIndex;

  /**
   * StaticComputationReuserImpl Implementation.
   * @param startTime an initial start time
   */
  @Inject
  private InfinitePartialTimespans(final TimescaleParser tsParser,
                                   @Parameter(StartTime.class) final long startTime) {
    LOG.info("START " + this.getClass());
    this.timescales = tsParser.timescales;
    this.startTime = startTime;
    this.partialTimespanMap = new HashMap<>();
    this.largestWindowSize = timescales.get(timescales.size()-1).windowSize;
    this.currIndex = startTime + incrementalStep + largestWindowSize;
    createSliceQueue();
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

      while (time <= incrementalStep + largestWindowSize) {
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
    //System.out.println("GET_NEXT_PARTIAL: " + adjStartTime(currTime) +", " + partialTimespanMap.get(adjStartTime(currTime)));
    return partialTimespanMap.get(currTime);
  }

  @Override
  public long getNextSliceTime(final long currTime) {
    // TODO: Incremental build
    //System.out.println("currTiume : " + currTime + ", currIndex: " + currIndex);
    if (currTime + largestWindowSize + incrementalStep > currIndex) {
      long nextStep = currTime + largestWindowSize + incrementalStep;
      //System.out.println("partial build: " + currIndex + ", "+ nextStep);
      //System.out.println("adjTime null:" + currTime + ", " + adjTime);
      final List<Long> sliceQueue = new LinkedList<>();
      for (final Timescale ts : timescales) {
        long adjStartTime = currIndex - (currIndex - startTime) % ts.intervalSize;
        boolean odd = false;
        while (adjStartTime <= nextStep) {
          final long pairedB = ts.windowSize % ts.intervalSize;
          final long pairedA = ts.intervalSize - pairedB;
          sliceQueue.add(adjStartTime);
          if (odd) {
            adjStartTime += pairedB;
          } else {
            adjStartTime += pairedA;
          }
          odd = !odd;
        }
      }
      Collections.sort(sliceQueue);
      //System.out.println(sliceQueue);
      long prevSliceTime = sliceQueue.get(0);
      for (final long nextSliceTime : sliceQueue) {
        if (prevSliceTime != nextSliceTime) {
          if (partialTimespanMap.get(prevSliceTime) == null) {
            //System.out.println("Add: " + prevSliceTime + ", " + nextSliceTime);
            partialTimespanMap.put(prevSliceTime, new Node<T>(prevSliceTime, nextSliceTime, true));
          }
          prevSliceTime = nextSliceTime;
        }
      }
      currIndex = nextStep;
    }
    Node<T> node = partialTimespanMap.get(currTime);
    return node.end;
  }

  @Override
  public String toString() {
    return partialTimespanMap.toString();
  }
}