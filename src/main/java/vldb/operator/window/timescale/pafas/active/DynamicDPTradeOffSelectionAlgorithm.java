package vldb.operator.window.timescale.pafas.active;

import org.apache.reef.tang.annotations.Parameter;
import vldb.operator.common.NotFoundException;
import vldb.operator.window.timescale.Timescale;
import vldb.operator.window.timescale.pafas.DependencyGraph;
import vldb.operator.window.timescale.pafas.Node;
import vldb.operator.window.timescale.pafas.PeriodCalculator;
import vldb.operator.window.timescale.pafas.dynamic.DynamicOutputLookupTable;
import vldb.operator.window.timescale.parameter.GCD;
import vldb.operator.window.timescale.parameter.StartTime;
import vldb.operator.window.timescale.parameter.TradeOffFactor;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Logger;

public class DynamicDPTradeOffSelectionAlgorithm<T> implements DependencyGraph.SelectionAlgorithm<T> {
  private final Logger LOG = Logger.getLogger(DynamicDPTradeOffSelectionAlgorithm.class.getName());
  private final DynamicActivePartialTimespans<T> partialTimespans;
  private final DynamicOutputLookupTable<Node<T>> finalTimespans;
  private final long period;
  private final long startTime;
  private final long gcd;
  private final double tradeOffFactor;

  @Inject
  private DynamicDPTradeOffSelectionAlgorithm(final DynamicActivePartialTimespans<T> partialTimespans,
                                              final DynamicOutputLookupTable<Node<T>> finalTimespans,
                                              final PeriodCalculator periodCalculator,
                                              @Parameter(StartTime.class) long startTime,
                                              @Parameter(GCD.class) long gcd,
                                              @Parameter(TradeOffFactor.class) double tradeOffFactor) {
    this.partialTimespans = partialTimespans;
    this.finalTimespans = finalTimespans;
    this.startTime = startTime;
    this.period = periodCalculator.getPeriod();
    this.gcd = gcd;
    this.tradeOffFactor = tradeOffFactor;
  }

  /**
   * This is for active partials.
   * If there is a timsepan [0-3]
   * but 3 is an active slice, then
   * there is not partial timespan [2-3),
   * so we need to reduce the end time to 2.
   */
  private long getReducedEnd(final long end) {
    for (long index = end; index > 0; index--) {
      if (partialTimespans.isSlicable(index) && !partialTimespans.isActive(index)) {
        return index;
      }
    }
    return 1;
  }

  @Override
  public List<Node<T>> selection(final long start, final long end) {
    final long reducedEnd = getReducedEnd(end);
    final int length = (int)(reducedEnd - start);
    final List<Double> costArray = new ArrayList<>(length + 1);
    final List<Node<T>> nodeArray = new ArrayList<>(length + 1);

    // Initialize
    for (int i = 0; i < length + 1; i++) {
      if (i == length) {
        costArray.add(i, 0.0);
      } else {
        costArray.add(Double.POSITIVE_INFINITY);
      }
      nodeArray.add(i, null);
    }

    for (int currIndex = length - 1; currIndex >= 0; currIndex -= 1) {
      final List<Node<T>> availableNodes = getAvailableNodes(start, reducedEnd, currIndex, length);
      // Dynamic programming
      for (final Node<T> node : availableNodes) {
        final int nodeEndIndex = (int)(node.end - start);
        final int nodeStartIndex = (int)(node.start - start);

        final double cost = 1 + tradeOffCost(node) + costArray.get(nodeEndIndex);
        if (cost < costArray.get(nodeStartIndex)) {
          costArray.set(nodeStartIndex, cost);
          nodeArray.set(nodeStartIndex, node);
        }
      }
    }

    // Build the solution
    final List<Node<T>> solution = new LinkedList<>();
    int index = 0;
    while (index < length) {
      final Node<T> solutionNode = nodeArray.get(index);
      if (solutionNode == null) {
        // This is active partial point
        break;
      }

      solution.add(solutionNode);
      index += (solutionNode.end - solutionNode.start);
    }

    return solution;
  }

  private List<Node<T>> getAvailableNodes(final long start, final long end, final int currIndex, final int length) {
    final List<Node<T>> availableNodes = new LinkedList<>();
    ConcurrentMap<Long, ConcurrentMap<Timescale, Node<T>>> availableFinalNodes;
    final long scanStartPoint = start + currIndex;

    try {
      availableFinalNodes = finalTimespans.lookup(scanStartPoint);
    } catch (final NotFoundException e) {
      availableFinalNodes = new ConcurrentHashMap<>();
    }

    // Add available finals
    for (final Map.Entry<Long, ConcurrentMap<Timescale, Node<T>>> entry : availableFinalNodes.entrySet()) {
      final long nodeEndTime = entry.getKey();
      if (nodeEndTime <= end) {
        for (final Node<T> finalNode : entry.getValue().values()) {
          final long nodeStartTime = finalNode.start;
          if (nodeEndTime - nodeStartTime < length) {
            availableNodes.add(finalNode);
          }
        }
      }
    }

    // Add partials
    final Node<T> availablePartialNode = partialTimespans.getNextPartialTimespanNode(scanStartPoint);
    if (availablePartialNode != null) {
      availableNodes.add(availablePartialNode);
    }

    return availableNodes;
  }

  private double tradeOffCost(final Node<T> node) {
    if (tradeOffFactor >= 100000.0) {
      return 0;
    } else {
      return node.refCnt.get() == 0 ? 1 / tradeOffFactor : 0.0;
    }
  }
}