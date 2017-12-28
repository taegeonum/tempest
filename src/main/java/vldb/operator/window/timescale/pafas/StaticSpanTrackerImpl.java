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
package vldb.operator.window.timescale.pafas;

import org.apache.reef.tang.annotations.Parameter;
import vldb.evaluation.Metrics;
import vldb.operator.window.aggregator.CAAggregator;
import vldb.operator.window.timescale.TimeMonitor;
import vldb.operator.window.timescale.Timescale;
import vldb.operator.window.timescale.common.SpanTracker;
import vldb.operator.window.timescale.common.TimescaleParser;
import vldb.operator.window.timescale.common.Timespan;
import vldb.operator.window.timescale.parameter.StartTime;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.logging.Logger;

/**
 * A SpanTracker using Greedy selection algorithm.
 */
public final class StaticSpanTrackerImpl<I, T> implements SpanTracker<T> {
  private static final Logger LOG = Logger.getLogger(StaticSpanTrackerImpl.class.getName());

  /**
   * The list of timescale.
   */
  private List<Timescale> timescales;

  private final PartialTimespans<T> partialTimespans;

  private final DependencyGraph dependencyGraph;

  private final TimeMonitor timeMonitor;
  private Metrics metrics;

  private int numAgg = 0;
  private final CAAggregator<?, T> aggregateFunction;

  /**
   * DependencyGraphComputationReuser constructor.
   * @param tsParser timescale parser
   */
  @Inject
  private StaticSpanTrackerImpl(final TimescaleParser tsParser,
                                @Parameter(StartTime.class) final long startTime,
                                final DependencyGraph<T> dependencyGraph,
                                final PartialTimespans partialTimespans,
                                final Metrics metrics,
                                final CAAggregator<?, T> aggregateFunction,
                                final TimeMonitor timeMonitor) {
    this.timescales = tsParser.timescales;
    this.timeMonitor = timeMonitor;
    this.partialTimespans = partialTimespans;
    this.dependencyGraph = dependencyGraph;
    this.metrics = metrics;
    this.aggregateFunction = aggregateFunction;
  }

  @Override
  public long getNextSliceTime(final long st) {
    //System.out.println("GET_NEXT_SLICE_TIME: " + st);
    return partialTimespans.getNextSliceTime(st);
  }

  @Override
  public List<Timespan> getFinalTimespans(final long t) {
    return dependencyGraph.getFinalTimespans(t);
  }

  private T createIntermediateAgg(final Node<T> intermediateNode) {
    final List<Node<T>> intermediateChildren = intermediateNode.getDependencies();
    final Collection<T> intermediateAgg = new ArrayList<>(intermediateChildren.size());
    for (final Node<T> intermediateChild : intermediateChildren) {
      if (intermediateChild.intermediate && intermediateChild.getOutput() == null) {
        // Recursive construction
        intermediateChild.saveOutput(createIntermediateAgg(intermediateChild));
        metrics.storedInter += 1;
      } else if (intermediateChild.getOutput() == null) {
        throw new RuntimeException("null aggregate at: " + intermediateChild + ", when generating " + intermediateNode);
      }

      intermediateAgg.add(intermediateChild.getOutput());
      intermediateChild.decreaseRefCnt();

      if (intermediateChild.getOutput() == null) {
        if (intermediateChild.partial) {
          //System.out.println("Deleted " + dependentNode);
          metrics.storedPartial -= 1;
        } else if (intermediateChild.intermediate) {
          metrics.storedInter -= 1;
        } else {
          metrics.storedFinal -= 1;
        }
      }
    }

    return aggregateFunction.aggregate(intermediateAgg);
  }

  @Override
  public List<T> getDependentAggregates(final Timespan timespan) {
    final Node<T> node = dependencyGraph.getNode(timespan);
    //System.out.println("PARENT NODE: " + node);
    final List<Node<T>> dependentNodes = node.getDependencies();

    //System.out.println(timespan + " DEP_NODES: " + dependentNodes);
    final List<T> aggregates = new ArrayList<>(dependentNodes.size());
    for (final Node<T> dependentNode : dependentNodes) {
      if (dependentNode.end <= timespan.endTime) {
        // Do not count first outgoing edge

        if (dependentNode.intermediate && dependentNode.getOutput() == null) {
          // Consider intermediate aggregates
          dependentNode.saveOutput(createIntermediateAgg(dependentNode));
          metrics.storedInter += 1;
        }

        if (dependentNode.getOutput() == null) {
          throw new RuntimeException("null aggregate at: " + dependentNode + ", when generating " + timespan);
        }

        aggregates.add(dependentNode.getOutput());
        dependentNode.decreaseRefCnt();

        if (dependentNode.getOutput() == null) {
          if (dependentNode.partial) {
            //System.out.println("Deleted " + dependentNode);
            metrics.storedPartial -= 1;
          } else if (dependentNode.intermediate) {
            metrics.storedInter -= 1;
          } else {
            metrics.storedFinal -= 1;
          }
        }

      }
    }

    return aggregates;
  }

  @Override
  public void putAggregate(final T agg, final Timespan timespan) {
    final Node<T> node = dependencyGraph.getNode(timespan);
    //if (timespan.timescale == null) {
    //  System.out.println("PUT_PARTIAL: " + timespan.startTime + ", " + timespan.endTime + " node: " + node.start + ", " + node.end);
    //}
    if (node.getInitialRefCnt() != 0) {
      synchronized (node) {
        if (node.getInitialRefCnt() != 0) {
          if (node.partial) {
            //System.out.println("Added " + node);
            metrics.storedPartial += 1;
          } else {
            metrics.storedFinal += 1;
          }
          node.saveOutput(agg);
          //System.out.println("PUT_AGG: " + timespan + ", " + node);
          // after saving the output, notify the thread that is waiting for this output.
          node.notifyAll();
        }
      }
    }
  }

  @Override
  public void addSlidingWindow(final Timescale ts, final long addTime) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public void removeSlidingWindow(final Timescale ts, final long deleteTime) {
    throw new RuntimeException("Not implemented");
  }
}