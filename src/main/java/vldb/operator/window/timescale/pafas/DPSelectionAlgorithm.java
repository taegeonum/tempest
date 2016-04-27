package vldb.operator.window.timescale.pafas;

import org.apache.reef.tang.annotations.Parameter;
import vldb.operator.common.NotFoundException;
import vldb.operator.window.timescale.common.OutputLookupTable;
import vldb.operator.window.timescale.parameter.GCD;
import vldb.operator.window.timescale.parameter.StartTime;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

public class DPSelectionAlgorithm<T> implements DependencyGraph.SelectionAlgorithm<T> {

  private final PartialTimespans<T> partialTimespans;
  private final OutputLookupTable<Node<T>> finalTimespans;
  private final long period;
  private final long startTime;
  private final Map<Long, Map<Long, Node<T>>> nodeMemento;
  private final Map<Long, Map<Long, Integer>> aggNumMemento;
  private final long gcd;

  @Inject
  private DPSelectionAlgorithm(final PartialTimespans<T> partialTimespans,
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
      if (currentStart < startTime) {
        scanStartPoint = currentStart + period;
      } else {
        scanStartPoint = currentStart;
      }

      final List<Node<T>> availableNodes = new LinkedList<>();
      try {
        ConcurrentSkipListMap<Long, Node<T>> availableFinalNodes = finalTimespans.lookup(scanStartPoint);
        for (final long endTime: availableFinalNodes.keySet()) {
          final Node<T> finalNode = availableFinalNodes.get(endTime);
          availableNodes.add(finalNode);
        }
      } catch (NotFoundException e) {
        // Do Nothing
      }
      final Node<T> availablePartialNode = partialTimespans.getNextPartialTimespanNode(scanStartPoint);
      if (availablePartialNode != null) {
        availableNodes.add(availablePartialNode);
      }
      for (final Node<T> node : availableNodes) {
        final long beforeStart;
        if (currentStart < startTime) {
          beforeStart = node.end - period;
        } else {
          beforeStart = node.end;
        }
        if (dpTable.containsKey(beforeStart) && dpTable.get(beforeStart) != -1) {
          final int candidate = dpTable.get(beforeStart) + 1;
          if (dpTable.get(currentStart) == -1 || dpTable.get(currentStart) > candidate) {
            dpTable.put(currentStart, candidate);
            dpTableNode.put(currentStart, node);
          }
        }
      }
      currentStart -= gcd;
    }
    final List<Node<T>> childrenNodes = new LinkedList<>();
    currentStart = start;
    while (currentStart < end) {
      final Node<T> currentNode = dpTableNode.get(currentStart);
      System.out.println(currentNode);
      childrenNodes.add(currentNode);
      if (currentNode.start == currentStart) {
        currentStart = currentNode.end;
      } else if (currentNode.start == currentStart + period) {
        currentStart = currentNode.end - period;
      }
    }
    return childrenNodes;
  }

}