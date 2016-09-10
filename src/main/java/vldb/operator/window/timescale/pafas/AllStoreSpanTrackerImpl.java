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
import vldb.operator.window.timescale.Timescale;
import vldb.operator.window.timescale.common.SpanTracker;
import vldb.operator.window.timescale.common.TimescaleParser;
import vldb.operator.window.timescale.common.Timespan;
import vldb.operator.window.timescale.pafas.dynamic.DynamicDependencyGraph;
import vldb.operator.window.timescale.pafas.dynamic.DynamicPartialTimespans;
import vldb.operator.window.timescale.parameter.StartTime;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

/**
 * A SpanTracker using Greedy selection algorithm.
 */
public final class AllStoreSpanTrackerImpl<I, T> implements SpanTracker<T> {
  private static final Logger LOG = Logger.getLogger(AllStoreSpanTrackerImpl.class.getName());

  /**
   * The list of timescale.
   */
  private List<Timescale> timescales;

  private final PartialTimespans<T> partialTimespans;

  private final DependencyGraph dependencyGraph;

  /**
   * DependencyGraphComputationReuser constructor.
   * @param tsParser timescale parser
   */
  @Inject
  private AllStoreSpanTrackerImpl(final TimescaleParser tsParser,
                                  @Parameter(StartTime.class) final long startTime,
                                  final DynamicDependencyGraph<T> dependencyGraph,
                                  final DynamicPartialTimespans partialTimespans) {
    this.timescales = tsParser.timescales;
    this.partialTimespans = partialTimespans;
    this.dependencyGraph = dependencyGraph;
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
                System.out.println("WAIT: " + dependentNode +", final: " + timespan);
                dependentNode.wait();
                //System.out.println("AWAKE: " + dependentNode);
              } catch (InterruptedException e) {
                e.printStackTrace();
              }
            } else {
              aggregates.add(dependentNode.getOutput());
              //dependentNode.decreaseRefCnt();
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
    node.saveOutput(agg);
    //System.out.println("PUT_AGG: " + timespan + ", " + node);
    // after saving the output, notify the thread that is waiting for this output.
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