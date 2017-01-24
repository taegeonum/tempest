package atc.operator.window.timescale.pafas.dynamic;

import org.apache.reef.tang.annotations.Parameter;
import atc.operator.common.NotFoundException;
import atc.operator.window.timescale.Timescale;
import atc.operator.window.timescale.pafas.DependencyGraph;
import atc.operator.window.timescale.pafas.Node;
import atc.operator.window.timescale.pafas.PartialTimespans;
import atc.operator.window.timescale.pafas.PeriodCalculator;
import atc.operator.window.timescale.parameter.GCD;
import atc.operator.window.timescale.parameter.StartTime;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Logger;

public class DynamicDPSelectionAlgorithm<T> implements DependencyGraph.SelectionAlgorithm<T> {
  private final Logger LOG = Logger.getLogger(DynamicDPSelectionAlgorithm.class.getName());
  private final PartialTimespans<T> partialTimespans;
  private final DynamicOutputLookupTable<Node<T>> finalTimespans;
  private final long period;
  private final long startTime;
  private final Map<Long, Map<Long, Node<T>>> nodeMemento;
  private final Map<Long, Map<Long, Integer>> aggNumMemento;
  private final long gcd;

  @Inject
  private DynamicDPSelectionAlgorithm(final DynamicPartialTimespans<T> partialTimespans,
                                      final DynamicOutputLookupTable<Node<T>> finalTimespans,
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

  @Override
  public List<Node<T>> selection(final long start, final long end) {
    Map<Long, Integer> dpTable = new HashMap<>();
    Map<Long, Node<T>> dpTableNode = new HashMap<>();

    dpTable.put(end, 0);
    long currentStart = end - gcd;
    while (currentStart >= start) {
      dpTable.put(currentStart, -1);
      dpTableNode.put(currentStart, null);
      final long scanStartPoint, scanEndPoint;
      scanStartPoint = currentStart;
      final List<Node<T>> availableNodes = new LinkedList<>();
      ConcurrentMap<Long, ConcurrentMap<Timescale, Node<T>>> availableFinalNodes = null;
      try {
        availableFinalNodes = finalTimespans.lookup(scanStartPoint);
      } catch (final NotFoundException nf1) {
        availableFinalNodes = new ConcurrentHashMap<>();
      } finally {
        // Add startTime + period nodes
        if (scanStartPoint < startTime) {
          try {
            final Map<Long, ConcurrentMap<Timescale, Node<T>>> additionalNodes = finalTimespans.lookup(scanStartPoint + period);
            //LOG.info("@@@ after: " + additionalNodes);
            if (additionalNodes != null) {
              availableFinalNodes.putAll(additionalNodes);
              //LOG.info("@@@ after2: " + availableFinalNodes);
            }
          } catch (final NotFoundException nf) {
            // Do nothing
          }
        }
      }

      for (final Map.Entry<Long, ConcurrentMap<Timescale, Node<T>>> entry: availableFinalNodes.entrySet()) {
        final long endTime = entry.getKey();
        // [s    |i             e]
        //                   [p--]
        // All nodes are pre-stored to finalTimespans.
        // 1) The node can be itself
        // 2) The node cannot be included in the [start-end)
        // We need to avoid those for correct algorithm.
        final Map<Timescale, Node<T>> nodeMap = entry.getValue();
        for (final Node<T> finalNode : nodeMap.values()) {
          if (finalNode.start > end) {
            availableNodes.add(finalNode);
            break;
          } else {
            if (!((endTime - scanStartPoint) == (end - start)) &&
                !((currentStart >= startTime) && (entry.getKey() > end))) {
              availableNodes.add(finalNode);
              break;
            }
          }
        }
      }

      final Node<T> availablePartialNode = partialTimespans.getNextPartialTimespanNode(scanStartPoint);
      if (availablePartialNode != null) {
        availableNodes.add(availablePartialNode);
      }

      //if (start == 1252 && end == 1586) {
      //  System.out.println("available nodes: " + availableNodes);
      //}

      for (final Node<T> node : availableNodes) {
        final long beforeStart;
        if (node.start >= end) {
          beforeStart = node.end - period;
        } else {
          beforeStart = node.end;
        }
        if (dpTable.containsKey(beforeStart) && dpTable.get(beforeStart) != -1) {
          final int candidate = dpTable.get(beforeStart) + 1;
          if (dpTable.get(currentStart) == -1 || dpTable.get(currentStart) > candidate) {
            dpTable.put(currentStart, candidate);
            dpTableNode.put(currentStart, node);
            //System.out.println("PUT dpTableNode: " + currentStart + ", NODE: " + node);
          }
        }
      }
      currentStart -= gcd;

    }

    final List<Node<T>> childrenNodes = new LinkedList<>();
    currentStart = start;
    while (currentStart < end) {
      final Node<T> currentNode = dpTableNode.get(currentStart);
      //System.out.println("CURR START: " + currentStart + ", NODE: " + currentNode);
      //System.out.println(currentNode);
      childrenNodes.add(currentNode);
      if (currentNode == null) {
        System.out.println(" Execption: " + currentStart);
      }
      if (currentNode.start == currentStart) {
        currentStart = currentNode.end;
      } else if (currentNode.start == currentStart + period) {
        currentStart = currentNode.end - period;
      }
    }
    return childrenNodes;
  }
}