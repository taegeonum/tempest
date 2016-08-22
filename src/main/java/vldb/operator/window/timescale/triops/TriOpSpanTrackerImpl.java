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
import vldb.operator.window.timescale.common.SharedForkJoinPool;
import vldb.operator.window.timescale.common.SpanTracker;
import vldb.operator.window.timescale.common.TimescaleParser;
import vldb.operator.window.timescale.common.Timespan;
import vldb.operator.window.timescale.onthefly.OntheflySelectionAlgorithm;
import vldb.operator.window.timescale.pafas.DependencyGraph;
import vldb.operator.window.timescale.pafas.Node;
import vldb.operator.window.timescale.pafas.PartialTimespans;
import vldb.operator.window.timescale.pafas.dynamic.DynamicDPOutputLookupTableImpl;
import vldb.operator.window.timescale.pafas.dynamic.DynamicDependencyGraphImpl;
import vldb.operator.window.timescale.pafas.dynamic.DynamicOutputLookupTable;
import vldb.operator.window.timescale.pafas.dynamic.DynamicPartialTimespans;
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

  private final DynamicPartialTimespans<T> sharedPartialTimespans;

  private final Map<Timescale, DynamicDependencyGraphImpl<T>> timescaleGraphMap;
  private final Map<Timescale, DynamicPartialTimespans<T>> partialTimespansMap;

  private final List<DependencyGraph<T>> dependencyGraphs;

  private final List<PartialTimespans> partialTimespans;

  //private final ParallelTreeAggregator<I, T> intermediateAggregator;
  private final AggregationCounter aggregationCounter;

  private final CAAggregator<I, T> caAggregator;

  private final long largestWindow;

  private long lastRemoved;
  /**
   * DependencyGraphComputationReuser constructor.
   * @param tsParser timescale parser
   */
  @Inject
  private TriOpSpanTrackerImpl(final TimescaleParser tsParser,
                               @Parameter(StartTime.class) final long startTime,
                               final TriWeave triWeave,
                               final DynamicPartialTimespans<T> sharedPartialTimespans,
                               @Parameter(NumThreads.class) final int numThreads,
                               final CAAggregator<I, T> caAggregator,
                               final AggregationCounter aggregationCounter,
                               final SharedForkJoinPool sharedForkJoinPool) throws InjectionException {
    this.partialTimespans = new LinkedList<>();
    this.timescales = tsParser.timescales;
    this.lastRemoved = startTime;
    this.largestWindow = timescales.get(timescales.size()-1).windowSize;
    this.aggregationCounter = aggregationCounter;
    this.sharedPartialTimespans = sharedPartialTimespans;
    this.timescaleGraphMap = new HashMap<>();
    this.partialTimespansMap = new HashMap<>();
    this.dependencyGraphs = new LinkedList<>();
    this.caAggregator = caAggregator;
    //this.intermediateAggregator = new ParallelTreeAggregator<>(
    //    numThreads, numThreads * 2, caAggregator, sharedForkJoinPool.getForkJoinPool());
    final Set<TriWeave.Group> groups = triWeave.generateOptimizedPlan(timescales);
    for (final TriWeave.Group group : groups) {
      final List<Timescale> tss = group.timescales;
      //System.out.println("Group " + tss);
      Collections.sort(tss);

      final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
      jcb.bindImplementation(DependencyGraph.SelectionAlgorithm.class, OntheflySelectionAlgorithm.class);
      jcb.bindNamedParameter(StartTime.class, startTime+"");
      jcb.bindNamedParameter(TimescaleString.class, TimescaleParser.parseToString(tss));
      jcb.bindImplementation(PartialTimespans.class, DynamicPartialTimespans.class);
      jcb.bindImplementation(DynamicOutputLookupTable.class, DynamicDPOutputLookupTableImpl.class);

      final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
      final DynamicDependencyGraphImpl<T> dependencyGraph = injector.getInstance(DynamicDependencyGraphImpl.class);
      final DynamicPartialTimespans pt = injector.getInstance(DynamicPartialTimespans.class);
      partialTimespans.add(pt);
      dependencyGraphs.add(dependencyGraph);
      for (final Timescale ts : tss) {
        timescaleGraphMap.put(ts, dependencyGraph);
        partialTimespansMap.put(ts, pt);
      }
    }
  }

  @Override
  public long getNextSliceTime(final long st) {
    //System.out.println("GET_NEXT_SLICE_TIME: " + st);
    for (final PartialTimespans ts : partialTimespans) {
      ts.getNextSliceTime(st);
    }
    return sharedPartialTimespans.getNextSliceTime(st);
  }

  @Override
  public List<Timespan> getFinalTimespans(final long t) {
    final List<Timespan> timespans = new LinkedList<>();
    for (final DependencyGraph<T> dependencyGraph : dependencyGraphs) {
      timespans.addAll(dependencyGraph.getFinalTimespans(t));
    }
    //System.out.println("GET FT: " + t + ", " + timespans);

    // REMOVE SHARED PARTIAL
    while (lastRemoved < t - largestWindow) {
      final Node<T> partialNode = sharedPartialTimespans.getNextPartialTimespanNode(lastRemoved);
      if (partialNode != null) {
        final long end = partialNode.end;
        lastRemoved = end;
        if (end <= t - largestWindow) {
          sharedPartialTimespans.removeNode(partialNode.start);
          //System.out.println("@@@RM " + partialNode);
        }
      }
    }
    return timespans;
  }

  @Override
  public List<T> getDependentAggregates(final Timespan timespan) {
    final DynamicDependencyGraphImpl<T> dependencyGraph = timescaleGraphMap.get(timespan.timescale);
    final DynamicPartialTimespans<T> pt = partialTimespansMap.get(timespan.timescale);
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
                //System.out.println("timespan: " + timespan + ", st: " + st);
                final Node<T> sharedPartialNode = sharedPartialTimespans.getNextPartialTimespanNode(st);
                intermediateAggregates.add(sharedPartialNode.getOutput());
                st += (sharedPartialNode.end - sharedPartialNode.start);
                dependentNode.refCnt -= 1;
                if (dependentNode.refCnt == 0) {
                  pt.removeNode(dependentNode.start);
                }
                dependentNode.parents.remove(node);
                //sharedPartialNode.decreaseRefCnt();
              }
              // Get the intermediate result.
              //aggregationCounter.incrementFinalAggregation(realEnd, (List<Map>)intermediateAggregates);
              final T intermediateResult = caAggregator.aggregate(intermediateAggregates);
              dependentNode.saveOutput(intermediateResult);
              aggregates.add(dependentNode.getOutput());
              dependentNode.refCnt -= 1;
              if (dependentNode.refCnt == 0) {
                pt.removeNode(dependentNode.start);
              }
              dependentNode.parents.remove(node);
              //dependentNode.decreaseRefCnt();
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
              dependentNode.refCnt -= 1;
              if (dependentNode.refCnt == 0) {
                dependencyGraph.removeNode(dependentNode);
              }
              dependentNode.parents.remove(node);
              //dependentNode.decreaseRefCnt();
              break;
            }
          }
        }
      }
    }
    node.getDependencies().clear();
    //System.out.println("RETURN: " + aggregates);
    return aggregates;
  }

  @Override
  public void putAggregate(final T agg, final Timespan timespan) {
    final DynamicDependencyGraphImpl<T> dependencyGraph;
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
          } else {
            dependencyGraph.removeNode(node);
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