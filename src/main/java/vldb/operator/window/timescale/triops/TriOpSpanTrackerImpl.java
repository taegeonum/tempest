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
package vldb.operator.window.timescale.triops;

import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;
import vldb.operator.window.aggregator.CAAggregator;
import vldb.operator.window.timescale.Timescale;
import vldb.operator.window.timescale.common.*;
import vldb.operator.window.timescale.onthefly.OntheflySelectionAlgorithm;
import vldb.operator.window.timescale.pafas.*;
import vldb.operator.window.timescale.parameter.NumThreads;
import vldb.operator.window.timescale.parameter.StartTime;
import vldb.operator.window.timescale.parameter.TimescaleString;
import vldb.operator.window.timescale.profiler.AggregationCounter;

import javax.inject.Inject;
import java.util.*;
import java.util.logging.Logger;

/**
 * A SpanTracker using Greedy selection algorithm.
 */
public final class TriOpSpanTrackerImpl<I, T> implements SpanTracker<T> {
  private static final Logger LOG = Logger.getLogger(TriOpSpanTrackerImpl.class.getName());

  /**
   * The list of timescale.
   */
  private List<Timescale> timescales;

  private final PartialTimespans<T> sharedPartialTimespans;

  private final Map<Timescale, DependencyGraph<T>> timescaleGraphMap;

  private final List<DependencyGraph<T>> dependencyGraphs;

  private final ParallelTreeAggregator<I, T> intermediateAggregator;
  private final AggregationCounter aggregationCounter;
  /**
   * DependencyGraphComputationReuser constructor.
   * @param tsParser timescale parser
   */
  @Inject
  private TriOpSpanTrackerImpl(final TimescaleParser tsParser,
                               @Parameter(StartTime.class) final long startTime,
                               final TriWeave triWeave,
                               final PartialTimespans<T> sharedPartialTimespans,
                               @Parameter(NumThreads.class) final int numThreads,
                               final CAAggregator<I, T> caAggregator,
                               final AggregationCounter aggregationCounter,
                               final SharedForkJoinPool sharedForkJoinPool) throws InjectionException {
    this.timescales = tsParser.timescales;
    this.aggregationCounter = aggregationCounter;
    this.sharedPartialTimespans = sharedPartialTimespans;
    this.timescaleGraphMap = new HashMap<>();
    this.dependencyGraphs = new LinkedList<>();
    this.intermediateAggregator = new ParallelTreeAggregator<>(
        numThreads, numThreads * 2, caAggregator, sharedForkJoinPool.getForkJoinPool());
    final Set<TriWeave.Group> groups = triWeave.generateOptimizedPlan(timescales);
    for (final TriWeave.Group group : groups) {
      final List<Timescale> tss = group.timescales;
      Collections.sort(tss);

      final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
      jcb.bindImplementation(DependencyGraph.SelectionAlgorithm.class, OntheflySelectionAlgorithm.class);
      jcb.bindNamedParameter(StartTime.class, startTime+"");
      jcb.bindNamedParameter(TimescaleString.class, TimescaleParser.parseToString(tss));

      final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
      final DependencyGraph<T> dependencyGraph = injector.getInstance(StaticDependencyGraphImpl.class);
      dependencyGraphs.add(dependencyGraph);
      for (final Timescale ts : tss) {
        timescaleGraphMap.put(ts, dependencyGraph);
      }
    }
  }

  @Override
  public long getNextSliceTime(final long st) {
    //System.out.println("GET_NEXT_SLICE_TIME: " + st);
    return sharedPartialTimespans.getNextSliceTime(st);
  }

  @Override
  public List<Timespan> getFinalTimespans(final long t) {
    final List<Timespan> timespans = new LinkedList<>();
    for (final DependencyGraph<T> dependencyGraph : dependencyGraphs) {
      timespans.addAll(dependencyGraph.getFinalTimespans(t));
    }
    return timespans;
  }

  @Override
  public List<T> getDependentAggregates(final Timespan timespan) {
    final DependencyGraph<T> dependencyGraph = timescaleGraphMap.get(timespan.timescale);
    final Node<T> node = dependencyGraph.getNode(timespan);
    final List<Node<T>> dependentNodes = node.getDependencies();
    //System.out.println(timespan + " DEP_NODES: " + dependentNodes);
    final List<T> aggregates = new LinkedList<>();
    for (final Node<T> dependentNode : dependentNodes) {
      if (dependentNode.end <= timespan.endTime) {
        // Do not count first outgoing edge
        while (true) {
          synchronized (dependentNode) {
            if (dependentNode.getOutput() == null && dependentNode.partial) {
              // This is partial. We should fetch aggregates from shared partials.
              // IntermediateAggregator should aggregate the shared partials and store the result to this node.
              // First, get real timespan
              long realStart = dependentNode.start;
              long realEnd = dependentNode.end;
              while (!(realStart >= timespan.startTime && realEnd <= timespan.endTime)) {
                realStart += dependencyGraph.getPeriod();
                realEnd += dependencyGraph.getPeriod();
              }
              //System.out.println("REAL_TS: [" + realStart + ", " + realEnd + ")" + " from [" + dependentNode.start + ", " + dependentNode.end + ")");
              // Fetch intermideate aggregates from shared partial timespans
              final List<T> intermediateAggregates = new LinkedList<>();
              long st = realStart;
              while (st < realEnd) {
                final Node<T> sharedPartialNode = sharedPartialTimespans.getNextPartialTimespanNode(st);
                intermediateAggregates.add(sharedPartialNode.getOutput());
                st += (sharedPartialNode.end - sharedPartialNode.start);
                sharedPartialNode.decreaseRefCnt();
              }
              // Get the intermediate result.
              aggregationCounter.incrementFinalAggregation(realEnd, (List<Map>)intermediateAggregates);
              final T intermediateResult = intermediateAggregator.doParallelAggregation(intermediateAggregates);
              dependentNode.saveOutput(intermediateResult);
              aggregates.add(dependentNode.getOutput());
              dependentNode.decreaseRefCnt();
              break;
            } else if (dependentNode.getOutput() == null) {
              // FinalAggregate is null. We should wait
              try {
                //System.out.println("WAIT: " + dependentNode);
                dependentNode.wait();
                //System.out.println("AWAKE: " + dependentNode);
              } catch (InterruptedException e) {
                e.printStackTrace();
              }
            } else {
              //System.out.println("DEP_NODE: " + dependentNode + ", V: " + dependentNode.getOutput());
              aggregates.add(dependentNode.getOutput());
              dependentNode.decreaseRefCnt();
              break;
            }
          }
        }
      }
    }
    //System.out.println("RETURN: " + aggregates);
    return aggregates;
  }

  @Override
  public void putAggregate(final T agg, final Timespan timespan) {
    final DependencyGraph<T> dependencyGraph;
    if (timespan.timescale == null) {
      // This is a shared partial agggregate. We need to store it into the sharedPartialTimespan.
      sharedPartialTimespans.getNextPartialTimespanNode(timespan.startTime).saveOutput(agg);
    } else {
      // This is a final aggregate.
      dependencyGraph = timescaleGraphMap.get(timespan.timescale);
      final Node<T> node = dependencyGraph.getNode(timespan);
      if (node.getInitialRefCnt() != 0) {
        synchronized (node) {
          if (node.getInitialRefCnt() != 0) {
            node.saveOutput(agg);
            //System.out.println("PUT_AGG: " + timespan + ", " + node + ", " + agg);
            // after saving the output, notify the thread that is waiting for this output.
            node.notifyAll();
          }
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