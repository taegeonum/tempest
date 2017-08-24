package vldb.operator.window.timescale.pafas;

import org.apache.reef.tang.annotations.Parameter;
import vldb.operator.common.NotFoundException;
import vldb.operator.window.timescale.common.OutputLookupTable;
import vldb.operator.window.timescale.parameter.GCD;
import vldb.operator.window.timescale.parameter.StartTime;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Logger;

public class DPSelectionAlgorithm<T> implements DependencyGraph.SelectionAlgorithm<T> {
  private final Logger LOG = Logger.getLogger(DPSelectionAlgorithm.class.getName());
  private final ActivePartialTimespans<T> partialTimespans;
  private final OutputLookupTable<Node<T>> finalTimespans;
  private final long period;
  private final long startTime;
  private final Map<Long, Map<Long, Node<T>>> nodeMemento;
  private final Map<Long, Map<Long, Integer>> aggNumMemento;
  private final long gcd;

  @Inject
  private DPSelectionAlgorithm(final ActivePartialTimespans<T> partialTimespans,
                               final OutputLookupTable<Node<T>> finalTimespans,
                               final PeriodCalculator periodCalculator,
                               @Parameter(StartTime.class) long startTime,
                               @Parameter(GCD.class) long gcd) {
    this.partialTimespans = partialTimespans;
    this.finalTimespans = finalTimespans;
    this.startTime = startTime;
    this.period = periodCalculator.getPeriod();
    this.gcd = gcd;
    this.nodeMemento = new HashMap<>();
    this.aggNumMemento = new HashMap<>();
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
        final int nodeEndIndex = (int)(adjustTime(end, node.end) - start);
        final int nodeStartIndex = (int)(adjustTime(end, node.start) - start);

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

    return solution;
  }

  private List<Node<T>> getAvailableNodes(final long start, final long end, final int currIndex, final int length) {
    final List<Node<T>> availableNodes = new LinkedList<>();
    ConcurrentMap<Long, Node<T>> availableFinalNodes = null;
    final long scanStartPoint = start + currIndex;

    try {
      availableFinalNodes = finalTimespans.lookup(scanStartPoint);
    } catch (final NotFoundException e) {
      availableFinalNodes = new ConcurrentHashMap<>();
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

      if (finalNode.start > end) {
        availableNodes.add(finalNode);
      } else {
        if (!((endTime - scanStartPoint) == (end - start)) &&
            !((scanStartPoint >= startTime) && (entry.getKey() > end))) {
          availableNodes.add(finalNode);
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