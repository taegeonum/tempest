package vldb.operator.window.timescale.pafas;

import org.apache.reef.tang.annotations.Parameter;
import vldb.operator.common.NotFoundException;
import vldb.operator.window.timescale.common.OutputLookupTable;
import vldb.operator.window.timescale.parameter.StartTime;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

public class DPSelectionAlgorithm<T> implements DependencyGraph.SelectionAlgorithm<T> {

  private final PartialTimespans<T> partialTimespans;
  private final OutputLookupTable<Node<T>> finalTimespans;
  private final long period;
  private final long startTime;
  private final Map<Long, Map<Long, List<Node<T>>>> dpTable;

  @Inject
  private DPSelectionAlgorithm(final PartialTimespans<T> partialTimespans,
                               final OutputLookupTable<Node<T>> finalTimespans,
                               final PeriodCalculator periodCalculator,
                               @Parameter(StartTime.class) long startTime) {
    this.partialTimespans = partialTimespans;
    this.finalTimespans = finalTimespans;
    this.startTime = startTime;
    this.period = periodCalculator.getPeriod();
    this.dpTable = new HashMap<>();
  }

  private void insertDpTable(long start, long end, List<Node<T>> nodes) {
    if (!dpTable.containsKey(start)) {
      dpTable.put(start, new HashMap<Long, List<Node<T>>>());
    }
    if (!dpTable.get(start).containsKey(end)) {
      dpTable.get(start).put(end, nodes);
    }
  }

  @Override
  public List<Node<T>> selection(final long start, final long end) {

    if (start == end) {
      return new LinkedList<>();
    }
    if (dpTable.containsKey(start)) {
      if (dpTable.get(start).containsKey(end)) {
        return dpTable.get(start).get(end);
      }
    }
    try {
      final Node<T> finalNode = finalTimespans.lookup(start, end);
      final List<Node<T>> childrenNodes = new LinkedList<>(Arrays.asList(finalNode));
      insertDpTable(start, end, childrenNodes);
    } catch (NotFoundException e) {
      final Node<T> partialNode = partialTimespans.getNextPartialTimespanNode(start);
      if (partialNode.end == end) {
        final List<Node<T>> childrenNodes = new LinkedList<>(Arrays.asList(partialNode));
        insertDpTable(start, end, childrenNodes);
        return new LinkedList<>(Arrays.asList(partialNode));
      }
    }

    List<Node<T>> minimumNodes = null;
    Node<T> minimumTimespan = null;
    final List<Node<T>> availableNodes = new LinkedList<>();

    try {
      final ConcurrentSkipListMap<Long, Node<T>> availableFinalNodes = finalTimespans.lookup(start);
      for (final long endTime: availableFinalNodes.keySet()) {
        final Node<T> finalNode = availableFinalNodes.get(endTime);
        if (finalNode.end <= end) {
          availableNodes.add(finalNode);
        }
      }
    } catch (NotFoundException e) {

    }
    final Node<T> partialNode = partialTimespans.getNextPartialTimespanNode(start);
    if (partialNode.end <= end) {
      availableNodes.add(partialNode);
    }
    if(availableNodes.isEmpty()) {
      throw new IllegalStateException("Cannot construct dependency graph!");
    }
    for (final Node<T> node : availableNodes) {
      final List<Node<T>> candidateNodes = selection(node.end, end);
      if (minimumNodes == null) {
        minimumNodes = candidateNodes;
        minimumTimespan = node;
      } else if (minimumNodes.size() > candidateNodes.size()) {
        minimumNodes = candidateNodes;
        minimumTimespan = node;
      }
    }
    final List<Node<T>> childrenNodes = new LinkedList<>(minimumNodes);
    childrenNodes.add(minimumTimespan);
    insertDpTable(start, end, childrenNodes);
    return childrenNodes;
  }

}