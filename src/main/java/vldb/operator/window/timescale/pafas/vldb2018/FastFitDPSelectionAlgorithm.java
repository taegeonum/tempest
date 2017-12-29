package vldb.operator.window.timescale.pafas.vldb2018;

import org.apache.reef.tang.annotations.Parameter;
import vldb.operator.common.NotFoundException;
import vldb.operator.window.timescale.common.OutputLookupTable;
import vldb.operator.window.timescale.pafas.DependencyGraph;
import vldb.operator.window.timescale.pafas.Node;
import vldb.operator.window.timescale.pafas.PartialTimespans;
import vldb.operator.window.timescale.pafas.PeriodCalculator;
import vldb.operator.window.timescale.parameter.GCD;
import vldb.operator.window.timescale.parameter.StartTime;
import vldb.operator.window.timescale.parameter.TradeOffFactor;

import javax.inject.Inject;
import java.util.*;
import java.util.logging.Logger;

public class FastFitDPSelectionAlgorithm<T> implements DependencyGraph.SelectionAlgorithm<T> {
  private final Logger LOG = Logger.getLogger(FastFitDPSelectionAlgorithm.class.getName());
  private final PartialTimespans<T> partialTimespans;
  private final OutputLookupTable<Node<T>> finalTimespans;
  private final long period;
  private final long startTime;
  private final long gcd;
  private final double tradeOffFactor;

  @Inject
  private FastFitDPSelectionAlgorithm(final PartialTimespans<T> partialTimespans,
                                      final OutputLookupTable<Node<T>> finalTimespans,
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

  private long adjustTime(final long end, final long time) {
    if (end < time) {
      return time - period;
    } else {
      return time;
    }
  }

  @Override
  public List<Node<T>> selection(final long start, final long end) {
    // Reduce end for active partial timespans;
    final long reducedEnd = end;
    final int length = (int)(reducedEnd - start);
    final List<Double> costArray = new ArrayList<>(length + 1);
    final List<Node<T>> nodeArray = new ArrayList<>(length + 1);

    // Initialize
    for (int i = 0; i < length + 1; i++) {
      if (i == length) {
        costArray.add(i, 0.0);
      } else {
        costArray.add(i, Double.POSITIVE_INFINITY);
      }
      nodeArray.add(i, null);
    }

    for (int currIndex = length - 1; currIndex >= 0; currIndex -= 1) {
      final List<Node<T>> availableNodes = getAvailableNodes(start, reducedEnd, currIndex, length);

      // Dynamic programming
      for (final Node<T> node : availableNodes) {
        final int nodeEndIndex = (int)(adjustTime(reducedEnd, node.end) - start);
        final int nodeStartIndex = (int)(adjustTime(reducedEnd-1, node.start) - start);

        final double cost = 1 + costArray.get(nodeEndIndex);

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

    //System.out.println("[" + start + ", " + end + "): " + solution);

    return solution;
  }

  private List<Node<T>> getAvailableNodes(final long start, final long end, final int currIndex, final int length) {
    final List<Node<T>> availableNodes = new LinkedList<>();
    final Map<Long, Node<T>> availableFinalNodes = new HashMap<>();
    final long scanStartPoint = start + currIndex;
    try {
      final Map<Long, Node<T>> nodes = finalTimespans.lookup(scanStartPoint);
      availableFinalNodes.putAll(nodes);
    } catch (final NotFoundException e) {
    } finally {
      // Add startTime + period nodes
      if (scanStartPoint < startTime) {
        try {
          final Map<Long, Node<T>> additionalNodes = finalTimespans.lookup(scanStartPoint + period);
          if (additionalNodes != null) {
            availableFinalNodes.putAll(additionalNodes);
          }
        } catch (final NotFoundException nf) {
          // Do nothing
        }
      }
    }

    // Add available finals
    for (final Map.Entry<Long, Node<T>> entry : availableFinalNodes.entrySet()) {
      final Node<T> finalNode = entry.getValue();
      final long endTime = entry.getKey();

      if (!finalNode.isNotShared) {
        if (finalNode.start >= end) {
          availableNodes.add(finalNode);
        } else {
          if (!((endTime - scanStartPoint) == (end - start)) &&
              !((scanStartPoint < end) && (entry.getKey() > end))) {
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
}