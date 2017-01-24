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
package atc.operator.window.timescale.pafas;

import org.apache.reef.tang.annotations.Parameter;
import atc.operator.window.timescale.Timescale;
import atc.operator.window.timescale.common.TimescaleParser;
import atc.operator.window.timescale.parameter.StartTime;

import javax.inject.Inject;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/** This implementation is based on "On-the-fly Sharing for Streamed Aggregation" paper.
 * Reference: S. Krishnamurthy, C. Wu, and M. Franklin. On-the-fly sharing
 * for streamed aggregation. In ACM SIGMOD, 2006
 * It returns next slice time for slicing input stream into paired sliced window.
 */
public final class DefaultPartialTimespans<T> implements PartialTimespans {
  private static final Logger LOG = Logger.getLogger(DefaultPartialTimespans.class.getName());

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

  /**
   * StaticComputationReuserImpl Implementation.
   * @param startTime an initial start time
   */
  @Inject
  private DefaultPartialTimespans(final TimescaleParser tsParser,
                                  @Parameter(StartTime.class) final long startTime,
                                  final PeriodCalculator periodCalculator) {
    this.timescales = tsParser.timescales;
    this.period = periodCalculator.getPeriod();
    this.startTime = startTime;
    this.partialTimespanMap = new HashMap<>();
    LOG.log(Level.INFO, DefaultPartialTimespans.class + " started. PERIOD: " + period);
    createSliceQueue();
    //System.out.println("TS: " + timescales + ", QUEUE: " + partialTimespanMap);
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

      while (time <= period) {
        sliceQueue.add(startTime + time);
        if (odd) {
          time += pairedB;
        } else {
          time += pairedA;
        }
        odd = !odd;
      }

      Collections.sort(sliceQueue);
      long prevSliceTime = startTime;
      for (final long nextSliceTime : sliceQueue) {
        if (prevSliceTime != nextSliceTime) {
          partialTimespanMap.put(prevSliceTime, new Node<T>(prevSliceTime, nextSliceTime, true));
          prevSliceTime = nextSliceTime;
        }
      }
    }
    //System.out.println(sliceQueue);
    LOG.log(Level.FINE, "Sliced queue: " + partialTimespanMap);
  }

  @Override
  public Node<T> getNextPartialTimespanNode(final long currTime) {
    //System.out.println("GET_NEXT_PARTIAL: " + adjStartTime(currTime));
    return partialTimespanMap.get(adjStartTime(currTime));
  }

  @Override
  public long getNextSliceTime(final long currTime) {
    final long adjTime = adjStartTime(currTime);
    final Node<T> node = partialTimespanMap.get(adjTime);
    return node.end + (currTime - adjTime);
  }

  private long adjStartTime(final long time) {
    if (time < startTime) {
      return time + period;
    } else {
      return startTime + (time - startTime) % period;
    }
  }

  @Override
  public String toString() {
    return partialTimespanMap.toString();
  }
}