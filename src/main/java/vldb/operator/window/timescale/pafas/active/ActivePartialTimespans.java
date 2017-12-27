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
package vldb.operator.window.timescale.pafas.active;

import org.apache.reef.tang.annotations.Parameter;
import vldb.operator.window.timescale.Timescale;
import vldb.operator.window.timescale.common.TimescaleParser;
import vldb.operator.window.timescale.pafas.Node;
import vldb.operator.window.timescale.pafas.PartialTimespans;
import vldb.operator.window.timescale.pafas.PeriodCalculator;
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
public final class ActivePartialTimespans<T> implements PartialTimespans {
  private static final Logger LOG = Logger.getLogger(ActivePartialTimespans.class.getName());

  /**
   * The list of timescale.
   */
  private final List<Timescale> timescales;

  /**
   * A period of the repeated pattern.
   */
  private final long period;

  /**
   * A start time.
   */
  private final long startTime;

  /**
   * Map of start_time and next timespan map.
   */
  private final Map<Long, Node<T>> partialTimespanMap;

  private final Set<Long> activeSlices;

  /**
   * StaticComputationReuserImpl Implementation.
   * @param startTime an initial start time
   */
  @Inject
  private ActivePartialTimespans(final TimescaleParser tsParser,
                                 @Parameter(StartTime.class) final long startTime,
                                 final PeriodCalculator periodCalculator) {
    this.timescales = tsParser.timescales;
    this.period = periodCalculator.getPeriod();
    this.startTime = startTime;
    this.partialTimespanMap = new HashMap<>();
    this.activeSlices = new HashSet<>();
    buildSlice(startTime, period);
    LOG.log(Level.INFO, ActivePartialTimespans.class + " started. PERIOD: " + period);
    //System.out.println("TS: " + timescales + ", QUEUE: " + partialTimespanMap);
    //System.out.println("Active slices: " + activeSlices);
  }

  private Node<T> findBeforeNode(final long time) {
    for (long i = time; i >=0; i--) {
      final Node<T> node = partialTimespanMap.get(i);
      if (node != null) {
        return node;
      }
    }
    throw new RuntimeException("Not found findBEforeNode: " + time);
  }

  private void buildSlice(final long start, final long end) {
    long nextStep = end;
    //System.out.println("partial build: " + currIndex + ", "+ nextStep);
    //System.out.println("adjTime null:" + currTime + ", " + adjTime);

    long prevSlice;
    if (start == startTime) {
      prevSlice = start;
    } else {
      final Node<T> prevNode = findBeforeNode(start);
      prevSlice = prevNode.end;
    }

    for (long st = start; st <= end; st += 1) {
      if (slicableByOthers(st)) {
        // Build active partials!
        if (isActiveSlice(st) && (st != start && st != end)) {
          activeSlices.add(st);
        } else {
          if (prevSlice < st) {
            //System.out.println("ADD start" + start + ", end: " + end + " (" + prevSlice  +", " + st + ")");
            if (partialTimespanMap.get(prevSlice) == null) {
              //System.out.println("CREATE PARTIAL1: " + prevSlice + ", " + st);
              partialTimespanMap.put(prevSlice, new Node<T>(prevSlice, st, true));
            } else {
              System.out.println("EXIST " + partialTimespanMap.get(prevSlice) + ", expected: " + prevSlice + ", " + st);
              throw new RuntimeException("HAHA");
            }
            prevSlice = st;
          }
        }
      }
    }
  }

  private boolean isActiveSlice(final long time) {
    final long adjTime = adjStartTime(time);
    for (final Timescale timescale : timescales) {
      long tsStart = startTime - (timescale.windowSize - timescale.intervalSize);
      long elapsedTime = adjTime - tsStart;
      if (elapsedTime % timescale.intervalSize == 0) {
        return false;
      }
    }
    return true;
  }

  public boolean isSlicable(final long time) {
    final long adjTime = adjStartTime(time);
    return partialTimespanMap.containsKey(adjTime) || adjTime == period;
  }

  public boolean isActive(final long time) {
    final long adjTime = adjStartTime(time);
    return activeSlices.contains(adjTime);
  }

  private boolean slicableByOthers(final long time) {
    for (final Timescale timescale : timescales) {
      long pairedB = timescale.windowSize % timescale.intervalSize;
      final long pairedA = timescale.intervalSize - pairedB;

      if ((time - startTime) == 0 ||
          (time - startTime - pairedA) % timescale.intervalSize == 0 ||
          (time - startTime) % timescale.intervalSize == 0) {
        // this is slicable by other timescale
        //System.out.println("Time " + time + " Slicable by " + timescale + ", at start " + st);
        return true;
      }
    }
    return false;
  }

  @Override
  public Node<T> getNextPartialTimespanNode(final long currTime) {
    //System.out.println("GET_NEXT_PARTIAL: " + adjStartTime(currTime));
    return partialTimespanMap.get(adjStartTime(currTime));
  }


  @Override
  public long getNextSliceTime(final long currTime) {
    try {
      final long adjTime = adjStartTime(currTime);
      final Node<T> node = partialTimespanMap.get(adjTime);
      return node.end + (currTime - adjTime);
    } catch (final Exception e) {
      throw new RuntimeException("currtime: " + currTime + ", adjTime: " + adjStartTime(currTime));
    }
  }

  @Override
  public boolean removePartialNode(final long startTime) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public boolean addPartialNode(final long startTime, final long endTime) {
    throw new RuntimeException("Not implemented");
  }

  private long adjStartTime(final long time) {
    if (time < startTime) {
      return time + period;
    } else {
      return startTime + (time - startTime) % period;
    }
  }

  public void reset() {
    for (final Node<T> partialNode : partialTimespanMap.values()) {
      partialNode.reset();
    }
  }

  @Override
  public String toString() {
    return partialTimespanMap.toString();
  }
}