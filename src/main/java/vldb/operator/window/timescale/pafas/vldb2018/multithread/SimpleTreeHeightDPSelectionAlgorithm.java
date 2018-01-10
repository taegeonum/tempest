package vldb.operator.window.timescale.pafas.vldb2018.multithread;

import org.apache.reef.tang.annotations.Parameter;
import vldb.operator.common.NotFoundException;
import vldb.operator.window.timescale.common.OutputLookupTable;
import vldb.operator.window.timescale.pafas.DependencyGraph;
import vldb.operator.window.timescale.pafas.Node;
import vldb.operator.window.timescale.pafas.PartialTimespans;
import vldb.operator.window.timescale.pafas.PeriodCalculator;
import vldb.operator.window.timescale.pafas.vldb2018.singlethread.MultiThreadFinalAggregator;
import vldb.operator.window.timescale.parameter.GCD;
import vldb.operator.window.timescale.parameter.StartTime;
import vldb.operator.window.timescale.parameter.TradeOffFactor;

import javax.inject.Inject;
import java.util.*;
import java.util.logging.Logger;

public class SimpleTreeHeightDPSelectionAlgorithm<T> implements DependencyGraph.SelectionAlgorithm<T> {
  private final Logger LOG = Logger.getLogger(SimpleTreeHeightDPSelectionAlgorithm.class.getName());
  private final PartialTimespans<T> partialTimespans;
  private final OutputLookupTable<Node<T>> finalTimespans;
  private final long period;
  private final long startTime;
  private final long gcd;
  private final double tradeOffFactor;
  private final int threshold;
  private final int ptl;

  @Inject
  private SimpleTreeHeightDPSelectionAlgorithm(final PartialTimespans<T> partialTimespans,
                                               final OutputLookupTable<Node<T>> finalTimespans,
                                               final PeriodCalculator periodCalculator,
                                               @Parameter(StartTime.class) long startTime,
                                               @Parameter(GCD.class) long gcd,
                                               @Parameter(MultiThreadFinalAggregator.ParallelThreshold.class) final int threshold,
                                               @Parameter(MultiThreadFinalAggregator.LeafThreshold.class) final int ptl,
                                               @Parameter(TradeOffFactor.class) double tradeOffFactor) {
    this.partialTimespans = partialTimespans;
    this.finalTimespans = finalTimespans;
    this.startTime = startTime;
    this.period = periodCalculator.getPeriod();
    this.gcd = gcd;
    this.threshold = threshold;
    this.ptl = ptl;
    this.tradeOffFactor = tradeOffFactor;
  }

  private long adjustTime(final long end, final long time) {
    if (end < time) {
      return time - period;
    } else {
      return time;
    }
  }

  private Node<T> getNodeLessThanHeight(final List<Node<T>> nodes, final int height) {
    /*
    Node<T> n = null;
    for (final Node<T> node : nodes) {
      if (node.height < height) {
        n = node;
      } else {
        break;
      }
    }
    return n;
    */
    return nodes.get(nodes.size() - height + 1);
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
      if (i == 0) {
        costArray.add(i, 0.0);
      } else {
        costArray.add(i, Double.POSITIVE_INFINITY);
      }
      nodeArray.add(i, null);
    }

    for (int currIndex = length - 1; currIndex >= 0; currIndex -= 1) {
      final List<Node<T>> availableNodes = getAvailableNodes(start, reducedEnd, currIndex, (end-start));
              /*
      if (currIndex == 0) {
        // calculate cost

        // find smallest height
        availableNodes.sort(new Comparator<Node<T>>() {
          @Override
          public int compare(final Node<T> o1, final Node<T> o2) {
            if (o1.height < o2.height) {
              return -1;
            } else if (o1.height > o2.height) {
              return 1;
            } else {
              return 0;
            }
          }
        });

        final int largestHeight = availableNodes.get(availableNodes.size()-1).height;

        if (largestHeight > threshold) {
          // Get a node that node.height < threshold
          final Node<T> node = getNodeLessThanHeight(availableNodes, threshold);
          final int nodeEndIndex = (int)(adjustTime(reducedEnd, node.end) - start);
          final int nodeStartIndex = (int)(adjustTime(reducedEnd-1, node.start) - start);

          final double cost = 1 + costArray.get(nodeStartIndex);
          costArray.set(nodeEndIndex, cost);
          nodeArray.set(nodeEndIndex, node);
        } else {
          for (final Node<T> node : availableNodes) {

            final int nodeEndIndex = (int)(adjustTime(reducedEnd, node.end) - start);
            final int nodeStartIndex = (int)(adjustTime(reducedEnd-1, node.start) - start);

            final double cost = 1 + costArray.get(nodeStartIndex);
            if (cost < costArray.get(nodeEndIndex)) {
              costArray.set(nodeEndIndex, cost);
              nodeArray.set(nodeEndIndex, node);
            } else if (cost == costArray.get(nodeEndIndex)) {
              final Node<T> prevOpt = nodeArray.get(nodeEndIndex);
              if (node.end - node.start > prevOpt.end - prevOpt.start) {
                costArray.set(nodeEndIndex, cost);
                nodeArray.set(nodeEndIndex, node);
              }
            }
          }
        }

      } else {        */
        // Dynamic programming
        for (final Node<T> node : availableNodes) {
          final int nodeEndIndex = (int)(adjustTime(reducedEnd, node.end) - start);
          final int nodeStartIndex = (int)(adjustTime(reducedEnd-1, node.start) - start);

          final double cost;
          if (node.end == end) {
            cost = node.cost + costArray.get(nodeStartIndex);
          } else {
            cost = 1 + costArray.get(nodeStartIndex);
          }
          if (cost < costArray.get(nodeEndIndex)) {
            costArray.set(nodeEndIndex, cost);
            nodeArray.set(nodeEndIndex, node);
          } else if (cost == costArray.get(nodeEndIndex)) {
            final Node<T> prevOpt = nodeArray.get(nodeEndIndex);
            if (node.end - node.start > prevOpt.end - prevOpt.start) {
              costArray.set(nodeEndIndex, cost);
              nodeArray.set(nodeEndIndex, node);
            }
          }
        }
      //}
    }

    // Build the solution
    final List<Node<T>> solution = new LinkedList<>();
    int index = length;
    while (index > 0) {
      final Node<T> solutionNode = nodeArray.get(index);
      if (solutionNode == null) {
        // This is active partial point
        break;
      }

      solution.add(0, solutionNode);
      index -= (solutionNode.end - solutionNode.start);
    }

    //System.out.println("[" + start + ", " + end + "): " + solution);

    return solution;
  }

  private List<Node<T>> getAvailableNodes(final long start,
                                          final long end,
                                          final int currIndex,
                                          final long length) {
    final List<Node<T>> availableNodes = new LinkedList<>();
    final Map<Long, Node<T>> availableFinalNodes = new HashMap<>();
    // scanStartPoint is the end time
    final long scanEndPoint = end - currIndex;

    try {
      final Map<Long, Node<T>> nodes = finalTimespans.lookup(scanEndPoint);
      availableFinalNodes.putAll(nodes);
    } catch (final NotFoundException e) {
    } finally {
      // Add startTime + period nodes
      if (scanEndPoint <= startTime) {
        try {
          final Map<Long, Node<T>> additionalNodes = finalTimespans.lookup(scanEndPoint + period);
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

      if (!finalNode.isNotShared) {
        if ((start <= finalNode.start && finalNode.end <= end) ||
            (start + period <= finalNode.start && finalNode.end <= end + period )) {
          if (finalNode.end - finalNode.start < length) {
            availableNodes.add(finalNode);
          }
        }
      }
    }

    // Add partials
    long partialEndTime = 0;
    if (end - currIndex <= 0) {
      partialEndTime = end - currIndex + period;
    } else {
      partialEndTime = end - currIndex;
    }

    for (long i = partialEndTime - 1; i >= start; i--) {
      final Node<T> availablePartialNode = partialTimespans.getNextPartialTimespanNode(i);
      if (availablePartialNode != null) {
        if (availablePartialNode.end == partialEndTime) {
          availableNodes.add(availablePartialNode);
        }

        break;
      }
    }

    return availableNodes;
  }
}