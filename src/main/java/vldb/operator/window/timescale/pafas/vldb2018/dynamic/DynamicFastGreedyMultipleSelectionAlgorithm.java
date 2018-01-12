package vldb.operator.window.timescale.pafas.vldb2018.dynamic;

import org.apache.reef.tang.annotations.Parameter;
import vldb.operator.common.NotFoundException;
import vldb.operator.window.timescale.common.OutputLookupTable;
import vldb.operator.window.timescale.common.WindowTimeAndOutput;
import vldb.operator.window.timescale.pafas.DependencyGraph;
import vldb.operator.window.timescale.pafas.Node;
import vldb.operator.window.timescale.pafas.PartialTimespans;
import vldb.operator.window.timescale.pafas.PeriodCalculator;
import vldb.operator.window.timescale.pafas.dynamic.DynamicPartialTimespans;
import vldb.operator.window.timescale.parameter.StartTime;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;

public class DynamicFastGreedyMultipleSelectionAlgorithm<T> implements DependencyGraph.SelectionAlgorithm<T> {

  private final PartialTimespans<T> partialTimespans;
  private final DynamicFastGreedyOutputLookupTableImpl<Node<T>> finalTimespans;
  private final long period;
  private final long startTime;

  @Inject
  private DynamicFastGreedyMultipleSelectionAlgorithm(final DynamicPartialTimespans<T> partialTimespans,
                                                      final OutputLookupTable<Node<T>> finalTimespans,
                                                      final PeriodCalculator periodCalculator,
                                                      @Parameter(StartTime.class) long startTime) {
    this.partialTimespans = partialTimespans;
    this.finalTimespans = (DynamicFastGreedyOutputLookupTableImpl)finalTimespans;
    this.startTime = startTime;
    this.period = periodCalculator.getPeriod();
  }

  private List<Node<T>> selectionAtStart(final long firstStart,
                                         final long start,
                                         final long end) {

    final List<Node<T>> childNodes = new LinkedList<>();
    // find child nodes.
    long st = end;

    // First fetch a dependent node
    WindowTimeAndOutput<Node<T>> elem = null;
    try {
      // Why end-1? because the [start-end) can be stored in the final timespans.
      elem = finalTimespans.lookupLargestSizeOutput(firstStart+1, st);
      st = elem.startTime;
      childNodes.add(elem.output);
    } catch (final NotFoundException e) {
      // Fetch from partial
      for (long i = st - 1; i >= firstStart; i--) {
        final Node<T> partialTimespanNode = partialTimespans.getNextPartialTimespanNode(i);
        if (partialTimespanNode != null) {
          if (partialTimespanNode.end == st) {
            childNodes.add(partialTimespanNode);
            st = partialTimespanNode.start;
          }
          //break;
        }
      }
    }

    while (st > start) {
      elem = null;
      try {
        elem = finalTimespans.lookupLargestSizeOutput(start, st);
        childNodes.add(elem.output);
        st = elem.startTime;
      } catch (final NotFoundException e) {
        // Find from partial
        for (long i = st - 1; i >= start; i--) {
          final Node<T> partialTimespanNode = partialTimespans.getNextPartialTimespanNode(i);
          if (partialTimespanNode != null) {
            if (partialTimespanNode.end == st) {
              childNodes.add(partialTimespanNode);
              st = partialTimespanNode.start;
            }
            break;
          }
        }
      }
    }
    //System.out.println("[" + start + ", " + end + "): " + childNodes);
    return childNodes;
  }

  private List<Node<T>> getMin(final List<Node<T>> l1, final List<Node<T>> l2) {
    return l1.size() < l2.size() ? l1 : l2;
  }

  private List<Node<T>> selectFromENd(final long start, final long end) {
    final List<Node<T>> childNodes = new LinkedList<>();
    // find child nodes.
    long st = end;

    // First fetch a dependent node
    WindowTimeAndOutput<Node<T>> elem = null;
    try {
      // Why end-1? because the [start-end) can be stored in the final timespans.
      elem = finalTimespans.lookupLargestSizeOutput(start+1, st);
      st = elem.startTime;
      childNodes.add(elem.output);
    } catch (final NotFoundException e) {
      // Fetch from partial
      for (long i = st - 1; i >= start; i--) {
        final Node<T> partialTimespanNode = partialTimespans.getNextPartialTimespanNode(i);
        if (partialTimespanNode != null) {
          if (partialTimespanNode.end == st) {
            childNodes.add(partialTimespanNode);
            st = partialTimespanNode.start;
          }
          //break;
        }
      }
    }

    while (st > start) {
      elem = null;
      try {
        elem = finalTimespans.lookupLargestSizeOutput(start, st);
        childNodes.add(elem.output);
        st = elem.startTime;
      } catch (final NotFoundException e) {
        // Find from partial
        for (long i = st - 1; i >= start; i--) {
          final Node<T> partialTimespanNode = partialTimespans.getNextPartialTimespanNode(i);
          if (partialTimespanNode != null) {
            if (partialTimespanNode.end == st) {
              childNodes.add(partialTimespanNode);
              st = partialTimespanNode.start;
            }
            break;
          }
        }
      }
    }
    //System.out.println("[" + start + ", " + end + "): " + childNodes);
    return childNodes;
  }

  private List<Node<T>> selectFromStart(final long start, final long end) {
    final List<Node<T>> childNodes = new LinkedList<>();
    // find child nodes.
    long st = start;

    // First fetch a dependent node
    WindowTimeAndOutput<Node<T>> elem = null;
    try {
      // Why end-1? because the [start-end) can be stored in the final timespans.
      elem = finalTimespans.lookupLargestSizeStartOutput(st, end - 1);
      st = elem.endTime;
      childNodes.add(elem.output);
    } catch (final NotFoundException e) {
      // Fetch from partial
      final Node<T> partialTimespanNode =  partialTimespans.getNextPartialTimespanNode(st);
      if (partialTimespanNode != null) {
        childNodes.add(partialTimespanNode);
        st = partialTimespanNode.end;
      }
    }

    while (st < end) {
      elem = null;
      try {
        elem = finalTimespans.lookupLargestSizeStartOutput(st, end);
        childNodes.add(elem.output);
        st = elem.endTime;
      } catch (final NotFoundException e) {
        // Find from partial
        final Node<T> partialTimespanNode =  partialTimespans.getNextPartialTimespanNode(st);
        if (partialTimespanNode != null) {
          childNodes.add(partialTimespanNode);
          st = partialTimespanNode.end;
        }
      }
    }
    //System.out.println("[" + start + ", " + end + "): " + childNodes);
    return childNodes;
  }

  @Override
  public List<Node<T>> selection(final long start, final long end) {
    final List<Node<T>> firstSolution = selectFromENd(start, end);
    final List<Node<T>> secondSolution = selectFromStart(start, end);
    return firstSolution.size() < secondSolution.size() ? firstSolution : secondSolution;
  }
}
