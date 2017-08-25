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
import vldb.operator.window.timescale.common.SpanTracker;
import vldb.operator.window.timescale.common.TimescaleParser;
import vldb.operator.window.timescale.common.Timespan;
import vldb.operator.window.timescale.pafas.Node;
import vldb.operator.window.timescale.parameter.StartTime;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * A SpanTracker using Greedy selection algorithm.
 */
public final class DynamicAllStoreSpanTrackerImpl<I, T> implements SpanTracker<T> {
  private static final Logger LOG = Logger.getLogger(DynamicAllStoreSpanTrackerImpl.class.getName());

  /**
   * The list of timescale.
   */
  private List<Timescale> timescales;

  private final DynamicPartialTimespans<T> partialTimespans;

  private final DynamicDependencyGraph dependencyGraph;

  private final WindowManager windowManager;

  private long prevSliceTime;

  private final long largestWindowSize;

  private long latestRemoved;

  private final TimeMonitor timeMonitor;
  /**
   * DependencyGraphComputationReuser constructor.
   * @param tsParser timescale parser
   */
  @Inject
  private DynamicAllStoreSpanTrackerImpl(final TimescaleParser tsParser,
                                         @Parameter(StartTime.class) final long startTime,
                                         final DynamicDependencyGraph<T> dependencyGraph,
                                         final WindowManager windowManager,
                                         final TimeMonitor timeMonitor,
                                         final DynamicPartialTimespans partialTimespans) {
    this.timescales = tsParser.timescales;
    this.partialTimespans = partialTimespans;
    this.latestRemoved = startTime;
    this.timeMonitor = timeMonitor;
    this.largestWindowSize = timescales.get(timescales.size()-1).windowSize;
    this.windowManager = windowManager;
    this.prevSliceTime = startTime;
    this.dependencyGraph = dependencyGraph;
  }

  @Override
  public long getNextSliceTime(final long st) {
    //System.out.println("GET_NEXT_SLICE_TIME: " + st);
    while (latestRemoved < st - largestWindowSize) {
      final Node<T> partial = partialTimespans.getNextPartialTimespanNode(latestRemoved);
      timeMonitor.storedKey -= ((Map)partial.getOutput()).size();
      partialTimespans.removeNode(partial.start);
      latestRemoved = partial.end;

      // Remove final
      final List<Timespan> finals = dependencyGraph.getFinalTimespans(latestRemoved);
      for (final Timespan timespan : finals) {
        final Node<T> node = dependencyGraph.getNode(timespan);
        timeMonitor.storedKey -= ((Map)node.getOutput()).size();
        dependencyGraph.removeNode(node);
      }
    }
    prevSliceTime = st;
    return partialTimespans.getNextSliceTime(st);
  }

  @Override
  public List<Timespan> getFinalTimespans(final long t) {
    //System.out.println("Get final timespan: " + t);
    return dependencyGraph.getFinalTimespans(t);
  }

  @Override
  public List<T> getDependentAggregates(final Timespan timespan) {
    final Node<T> node = dependencyGraph.getNode(timespan);
    //System.out.println("PARENT NODE: " + node);
    final List<Node<T>> dependentNodes = node.getDependencies();
    //System.out.println(timespan + " DEP_NODES: " + dependentNodes);
    final List<T> aggregates = new LinkedList<>();
    for (final Node<T> dependentNode : dependentNodes) {
      if (dependentNode.end <= timespan.endTime) {
        // Do not count first outgoing edge
        while (true) {
          synchronized (dependentNode) {
            if (!dependentNode.outputStored.get()) {
              // null
              try {
                System.out.println("WAIT: " + dependentNode +", final: " + timespan + ", " + timespan.timescale);
                dependentNode.wait();
                //System.out.println("AWAKE: " + dependentNode);
              } catch (InterruptedException e) {
                e.printStackTrace();
              }
            } else {
              aggregates.add(dependentNode.getOutput());
              dependentNode.parents.remove(node);
              if (dependentNode.refCnt.decrementAndGet() == 0) {
                // Remove
                timeMonitor.storedKey -= ((Map)dependentNode.getOutput()).size();
                if (dependentNode.partial) {
                  //System.out.println("DELETE!!: " + dependentNode + ", for: " + timespan);
                  partialTimespans.removeNode(dependentNode.start);
                } else {
                  dependencyGraph.removeNode(dependentNode);
                }
              }
              break;
            }
          }
        }
      }
    }
    // Remove dependencies because it is no use any more
    node.getDependencies().clear();
    return aggregates;
  }

  @Override
  public void putAggregate(final T agg, final Timespan timespan) {
    final Node<T> node = dependencyGraph.getNode(timespan);
    timeMonitor.storedKey += ((Map)agg).size();
    node.saveOutput(agg);
  }

  @Override
  public void addSlidingWindow(final Timescale ts, final long addTime) {
    windowManager.addWindow(ts, addTime);
    partialTimespans.addWindow(ts, prevSliceTime, addTime);
    dependencyGraph.addSlidingWindow(ts, addTime);
  }

  @Override
  public void removeSlidingWindow(final Timescale ts, final long deleteTime) {
    final long stTime = windowManager.timescaleStartTime(ts);
    windowManager.removeWindow(ts, deleteTime);
    partialTimespans.removeWindow(ts, prevSliceTime, deleteTime, stTime);
    dependencyGraph.removeSlidingWindow(ts, stTime, deleteTime);
  }
}