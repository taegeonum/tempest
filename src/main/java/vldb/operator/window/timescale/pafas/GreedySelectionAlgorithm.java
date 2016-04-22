package vldb.operator.window.timescale.pafas;

import vldb.operator.common.NotFoundException;
import vldb.operator.window.timescale.common.OutputLookupTable;
import vldb.operator.window.timescale.common.WindowTimeAndOutput;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;

public class GreedySelectionAlgorithm<T> implements DependencyGraph.SelectionAlgorithm<T> {

  private final PartialTimespans<T> partialTimespans;
  private final OutputLookupTable<Node<T>> finalTimespans;
  private final long period;

  @Inject
  private GreedySelectionAlgorithm(final PartialTimespans<T> partialTimespans,
                                   final OutputLookupTable<Node<T>> finalTimespans,
                                   final PeriodCalculator periodCalculator) {
    this.partialTimespans = partialTimespans;
    this.finalTimespans = finalTimespans;
    this.period = periodCalculator.getPeriod();
  }

  @Override
  public List<Node<T>> selection(final long start, final long end) {
    final List<Node<T>> childNodes = new LinkedList<>();
    // find child nodes.
    long st = start;
    while (st < end) {
      WindowTimeAndOutput<Node<T>> elem = null;
      try {
        elem = finalTimespans.lookupLargestSizeOutput(st, end);

        if (st == elem.endTime) {
          break;
        } else {
          childNodes.add(elem.output);
          st = elem.endTime;
        }
      } catch (final NotFoundException e) {
        try {
          // Find outgoing edges
          elem = finalTimespans.lookupLargestSizeOutput(st + period, period);
          childNodes.add(elem.output);
          st = elem.endTime - period;
        } catch (final NotFoundException e1) {
          // do nothing
        }
      }

      // No relations among final timespans, so find from partial timespans
      if (elem == null) {
        if (st < 0) {
          final Node<T> partialTimespanNode = partialTimespans.getNextPartialTimespanNode(st + period);
          childNodes.add(partialTimespanNode);
          st = partialTimespanNode.end - period;
        } else {
          final Node<T> partialTimespanNode = partialTimespans.getNextPartialTimespanNode(st);
          childNodes.add(partialTimespanNode);
          st = partialTimespanNode.end;
        }
      }
    }
    //System.out.println("[" + start + ", " + end + "): " + childNodes);
    return childNodes;
  }
}
