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
package atc.operator.window.timescale.pafas.dynamic;

import org.apache.reef.tang.annotations.Parameter;
import atc.operator.window.timescale.TimeMonitor;
import atc.operator.window.timescale.Timescale;
import atc.operator.window.timescale.common.SpanTracker;
import atc.operator.window.timescale.common.TimescaleParser;
import atc.operator.window.timescale.common.Timespan;
import atc.operator.window.timescale.pafas.Node;
import atc.operator.window.timescale.parameter.StartTime;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

/**
 * A SpanTracker using Greedy selection algorithm.
 */
public final class DynamicSpanTrackerImpl<I, T> implements SpanTracker<T> {
  private static final Logger LOG = Logger.getLogger(DynamicSpanTrackerImpl.class.getName());

  /**
   * The list of timescale.
   */
  private List<Timescale> timescales;

  private final DynamicPartialTimespans<T> partialTimespans;

  private final DynamicDependencyGraph dependencyGraph;

  private final WindowManager windowManager;

  private long lastRemoved;

  private long prevSliceTime;

  private long largestWindowSize;

  private final TimeMonitor timeMonitor;
  /**
   * DependencyGraphComputationReuser constructor.
   * @param tsParser timescale parser
   */
  @Inject
  private DynamicSpanTrackerImpl(final TimescaleParser tsParser,
                                 @Parameter(StartTime.class) final long startTime,
                                 final DynamicDependencyGraph<T> dependencyGraph,
                                 final WindowManager windowManager,
                                 final TimeMonitor timeMonitor,
                                 final DynamicPartialTimespans partialTimespans) {
    this.timescales = tsParser.timescales;
    this.partialTimespans = partialTimespans;
    this.windowManager = windowManager;
    this.prevSliceTime = startTime;
    this.lastRemoved = startTime;
    this.largestWindowSize = timescales.get(timescales.size()-1).windowSize;
    this.dependencyGraph = dependencyGraph;
    this.timeMonitor = timeMonitor;
  }

  @Override
  public long getNextSliceTime(final long st) {
    //System.out.println("GET_NEXT_SLICE_TIME: " + st);
    prevSliceTime = st;
    return partialTimespans.getNextSliceTime(st);
  }

  @Override
  public List<Timespan> getFinalTimespans(final long t) {
    //System.out.println("Get final timespan: " + t);
    // Remove partial and final
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
      aggregates.add(dependentNode.getOutput());
      dependentNode.parents.remove(node);
      if (dependentNode.refCnt.decrementAndGet() == 0) {
        if (dependentNode.partial) {
          partialTimespans.removeNode(dependentNode.start);
        } else {
          dependencyGraph.removeNode(dependentNode);
        }
      }
    }
    // Remove dependencies because it is no use any more
    node.getDependencies().clear();
    return aggregates;
  }

  @Override
  public List<Node<T>> getDependentNodes(final Timespan timespan) {
    final Node<T> node = dependencyGraph.getNode(timespan);
    //System.out.println("PARENT NODE: " + node);
    return node.getDependencies();
  }

  @Override
  public void putAggregate(final T agg, final Timespan timespan) {
    final Node<T> node = dependencyGraph.getNode(timespan);
    //if (timespan.timescale == null) {
    //  System.out.println("PUT_PARTIAL: " + timespan.startTime + ", " + timespan.endTime + " node: " + node.start + ", " + node.end);
    //}
    //System.out.println("PUT " + timespan +", to " + node);
    if (node.refCnt.get() != 0) {
      node.saveOutput(agg);
    } else {
      // Remove the node from DAG if it is no use.
      dependencyGraph.removeNode(node);
    }
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