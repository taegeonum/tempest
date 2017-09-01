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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * A SpanTracker using Greedy selection algorithm.
 */
public final class EagerStaticSpanTrackerImpl<I, T> implements SpanTracker<T> {
  private static final Logger LOG = Logger.getLogger(EagerStaticSpanTrackerImpl.class.getName());

  /**
   * The list of timescale.
   */
  private List<Timescale> timescales;

  private final PartialTimespans<T> partialTimespans;

  private final DependencyGraph dependencyGraph;

  private final TimeMonitor timeMonitor;
  private Metrics metrics;

  private final CAAggregator<?, T> aggregator;

  /**
   * DependencyGraphComputationReuser constructor.
   * @param tsParser timescale parser
   */
  @Inject
  private EagerStaticSpanTrackerImpl(final TimescaleParser tsParser,
                                     @Parameter(StartTime.class) final long startTime,
                                     final DependencyGraph<T> dependencyGraph,
                                     final PartialTimespans partialTimespans,
                                     final CAAggregator<?, T> aggregator,
                                     final Metrics metrics,
                                     final TimeMonitor timeMonitor) {
    this.timescales = tsParser.timescales;
    this.timeMonitor = timeMonitor;
    this.partialTimespans = partialTimespans;
    this.dependencyGraph = dependencyGraph;
    this.metrics = metrics;
    this.aggregator = aggregator;
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

  @Override
  public List<T> getDependentAggregates(final Timespan timespan) {
    final Node<T> node = dependencyGraph.getNode(timespan);
    final List<T> l = new ArrayList<>(2);
    l.add(node.getOutput());
    return l;
  }

  private T copyAgg(final T agg) {
    final Map<Object, Object> result = new HashMap<>();
    final Map<Object, Object> e = (Map)agg;
    for (final Map.Entry<Object, Object> entry : e.entrySet()) {
      result.put(entry.getKey(), entry.getValue());
    }
    return (T)result;
  }

  @Override
  public void putAggregate(final T agg, final Timespan timespan) {
    // Eagerly perform aggregation!!

    final Node<T> node = dependencyGraph.getNode(timespan);
    if (node.getOutput() != null) {
      node.saveOutput(null);
      node.outputStored.set(false);
      metrics.storedFinal -= 1;
      //System.out.println("DELETED: " + node);
    }

    for (final Node<T> parent : node.parents) {
      final T output = parent.getOutput();
      if (output == null) {
        parent.saveOutput(copyAgg(agg));
        metrics.storedFinal += 1;
        //System.out.println("ADDED: " + parent);
      } else {
        final T o = aggregator.rollup(output, agg);
        parent.saveOutput(o);
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