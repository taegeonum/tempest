package vldb.operator.window.timescale.pafas.dynamic;

import org.apache.reef.tang.annotations.Parameter;
import vldb.operator.common.NotFoundException;
import vldb.operator.window.timescale.Timescale;
import vldb.operator.window.timescale.pafas.DependencyGraph;
import vldb.operator.window.timescale.pafas.Node;
import vldb.operator.window.timescale.pafas.PartialTimespans;
import vldb.operator.window.timescale.pafas.PeriodCalculator;
import vldb.operator.window.timescale.parameter.GCD;
import vldb.operator.window.timescale.parameter.StartTime;
import vldb.operator.window.timescale.parameter.TradeOffFactor;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Logger;

public class DynamicDPTradeOffSelectionAlgorithm<T> implements DependencyGraph.SelectionAlgorithm<T> {
  private final Logger LOG = Logger.getLogger(DynamicDPTradeOffSelectionAlgorithm.class.getName());
  private final PartialTimespans<T> partialTimespans;
  private final DynamicOutputLookupTable<Node<T>> finalTimespans;
  private final long period;
  private final long startTime;
  private final long gcd;
  private final double tradeOffFactor;

  @Inject
  private DynamicDPTradeOffSelectionAlgorithm(final DynamicPartialTimespans<T> partialTimespans,
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

  @Override
  public List<Node<T>> selection(final long start, final long end) {
    final int length = (int)(end - start);
    final List<Double> costArray = new ArrayList<>(length + 1);
    final List<Node<T>> nodeArray = new ArrayList<>(length + 1);

    // Initialize
    for (int i = 0; i < length + 1; i++) {
      if (i == length) {
        costArray.set(i, 0.0);
      } else {
        costArray.set(i, Double.POSITIVE_INFINITY);
      }
      nodeArray.set(i, null);
    }

    for (int currIndex = length - 1; currIndex >= 0; currIndex -= 1) {
      final List<Node<T>> availableNodes = getAvailableNodes(start, end, currIndex, length);
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
      solution.add(solutionNode);
      index += (solutionNode.end - solutionNode.start);
    }
    return solution;
  }

  private List<Node<T>> getAvailableNodes(final long start, final long end, final int currIndex, final int length) {
    final List<Node<T>> availableNodes = new LinkedList<>();
    ConcurrentMap<Long, ConcurrentMap<Timescale, Node<T>>> availableFinalNodes;

    try {
      availableFinalNodes = finalTimespans.lookup(currIndex + start);
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
    final Node<T> availablePartialNode = partialTimespans.getNextPartialTimespanNode(currIndex + start);
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