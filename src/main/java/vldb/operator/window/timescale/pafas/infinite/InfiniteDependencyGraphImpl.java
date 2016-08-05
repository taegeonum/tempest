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
import vldb.operator.common.NotFoundException;
import vldb.operator.window.timescale.Timescale;
import vldb.operator.window.timescale.common.OutputLookupTable;
import vldb.operator.window.timescale.common.TimescaleParser;
import vldb.operator.window.timescale.common.Timespan;
import vldb.operator.window.timescale.pafas.DependencyGraph;
import vldb.operator.window.timescale.pafas.Node;
import vldb.operator.window.timescale.pafas.PartialTimespans;
import vldb.operator.window.timescale.pafas.PeriodCalculator;
import vldb.operator.window.timescale.parameter.StartTime;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * DependencyGraph shows which nodes are related to each other. This is used so that unnecessary outputs are not saved.
 */
public final class InfiniteDependencyGraphImpl<T> implements DependencyGraph {

  private static final Logger LOG = Logger.getLogger(InfiniteDependencyGraphImpl.class.getName());

  /**
   * A table containing DependencyGraphNode for partial outputs.
   */
  //private final DefaultOutputLookupTableImpl<DependencyGraphNode> partialOutputTable;

  /**
   * The list of timescale.
   */
  private final List<Timescale> timescales;

  /**
   * A start time.
   */
  private final long startTime;

  private final PartialTimespans<T> partialTimespans;
  private final OutputLookupTable<Node<T>> finalTimespans;
  private final SelectionAlgorithm<T> selectionAlgorithm;

  private long currBuildingIndex;
  private final long largestWindowSize;
  private final long period;

  /**
   * DependencyGraph constructor. This first builds the dependency graph.
   * @param startTime the initial start time of when the graph is built.
   */
  @Inject
  private InfiniteDependencyGraphImpl(final TimescaleParser tsParser,
                                      @Parameter(StartTime.class) final long startTime,
                                      final PartialTimespans partialTimespans,
                                      final OutputLookupTable<Node<T>> outputLookupTable,
                                      final SelectionAlgorithm<T> selectionAlgorithm,
                                      final PeriodCalculator periodCalculator) {
    LOG.info("START " + this.getClass());
    this.partialTimespans = partialTimespans;
    this.timescales = tsParser.timescales;
    this.largestWindowSize = timescales.get(timescales.size()-1).windowSize;
    this.startTime = startTime;
    this.currBuildingIndex = startTime + largestWindowSize;
    this.finalTimespans = outputLookupTable;
    this.selectionAlgorithm = selectionAlgorithm;
    this.period = periodCalculator.getPeriod();
    addOverlappingWindowNodeAndEdge(startTime, startTime + largestWindowSize);
  }

  /**
   * Add DependencyGraphNode and connect dependent nodes.
   */
  private void addOverlappingWindowNodeAndEdge(final long curr, final long until) {
    final List<Node> addedNodes = new LinkedList<>();
    for (final Timescale timescale : timescales) {
      final long align = curr + (timescale.intervalSize - (curr - startTime) % timescale.intervalSize);
      for (long time = align; time <= until; time += timescale.intervalSize) {
        // create vertex and add it to the table cell of (time, windowsize)
        final long start = time - timescale.windowSize;

        Node parent;
        try {
          parent = finalTimespans.lookup(start, time);
        } catch (NotFoundException e) {
          parent = new Node(start, time, false);
          finalTimespans.saveOutput(start, time, parent);
          addedNodes.add(parent);
        }
        //System.out.println("ADD: " + start + "-" + time);
        //System.out.println("SAVE: [" + start + "-" + time + ")");

        LOG.log(Level.FINE, "Saved to table : (" + start + " , " + time + ") refCount : " + parent.refCnt);
      }
    }

    // Add edges
    for (final Node parent : addedNodes) {
      final List<Node<T>> childNodes = selectionAlgorithm.selection(Math.max(parent.start, startTime), parent.end);
      LOG.log(Level.FINE, "(" + parent.start + ", " + parent.end + ") dependencies1: " + childNodes);
      for (final Node<T> elem : childNodes) {
        parent.addDependency(elem);
      }
    }
  }

  @Override
  public List<Timespan> getFinalTimespans(final long t) {

    /**
     * [-------------------- period -----------------------]
     * 1)      ^       ^  <--------------->
     *         t      cbi        lw
     *
     * 2)              ^  <---------------> <----->
     *               t,cbi       lw          step
     *
     * 3)              ^ <----->  ^ <------------->
     *                 t   step  cbi     lw
     */
    final List<Timespan> timespans = new LinkedList<>();
    if (t + largestWindowSize > currBuildingIndex) {
      // Incremental build dependency graph
      addOverlappingWindowNodeAndEdge(currBuildingIndex, t + largestWindowSize);
      currBuildingIndex = t + largestWindowSize;
    }

    for (final Timescale timescale : timescales) {
      if ((t - startTime) % timescale.intervalSize == 0) {
        timespans.add(new Timespan(t - timescale.windowSize, t, timescale));
      }
    }
    return timespans;
  }

  @Override
  public Node<T> getNode(final Timespan timespan) {
    long adjStartTime = timespan.startTime;
    final long adjEndTime = timespan.endTime;
    //System.out.println("ORIGIN: " + timespan + ", ADJ: [" + adjStartTime + ", " + adjEndTime + ")");

    try {
      return finalTimespans.lookup(adjStartTime, adjEndTime);
    } catch (final NotFoundException e) {
      // find partial timespan
      final Node<T> partialTimespanNode = partialTimespans.getNextPartialTimespanNode(adjStartTime);
      if (partialTimespanNode.end != adjEndTime) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
      return partialTimespanNode;
    }
  }

  @Override
  public long getPeriod() {
    return period;
  }

  @Override
  public void addSlidingWindow(final Timescale ts, final long addTime) {

  }

  @Override
  public void removeSlidingWindow(final Timescale ts, final long deleteTime) {

  }
}
