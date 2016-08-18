package vldb.operator.window.timescale.pafas.dynamic;

import org.apache.reef.tang.annotations.Parameter;
import vldb.operator.common.NotFoundException;
import vldb.operator.window.timescale.common.WindowTimeAndOutput;
import vldb.operator.window.timescale.pafas.DependencyGraph;
import vldb.operator.window.timescale.pafas.Node;
import vldb.operator.window.timescale.pafas.PartialTimespans;
import vldb.operator.window.timescale.pafas.PeriodCalculator;
import vldb.operator.window.timescale.parameter.StartTime;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;

public class DynamicGreedySelectionAlgorithm<T> implements DependencyGraph.SelectionAlgorithm<T> {

  private final PartialTimespans<T> partialTimespans;
  private final DynamicOutputLookupTable<Node<T>> finalTimespans;
  private final long period;
  private final long startTime;

  @Inject
  private DynamicGreedySelectionAlgorithm(final PartialTimespans<T> partialTimespans,
                                          final DynamicOutputLookupTable<Node<T>> finalTimespans,
                                          final PeriodCalculator periodCalculator,
                                          @Parameter(StartTime.class) long startTime) {
    this.partialTimespans = partialTimespans;
    this.finalTimespans = finalTimespans;
    this.startTime = startTime;
    this.period = periodCalculator.getPeriod();
  }

  @Override
  public List<Node<T>> selection(final long start, final long end) {
    final List<Node<T>> childNodes = new LinkedList<>();
    // find child nodes.
    long st = start;

    // First fetch a dependent node
    WindowTimeAndOutput<Node<T>> elem = null;
    try {
      // Why end-1? because the [start-end) can be stored in the final timespans.
      elem = finalTimespans.lookupLargestSizeOutput(st, end-1);
      st = elem.endTime;
      childNodes.add(elem.output);
    } catch (final NotFoundException e) {
      // Fetch from partial
      if (st < startTime) {
        final Node<T> partialTimespanNode = partialTimespans.getNextPartialTimespanNode(st + period);
        childNodes.add(partialTimespanNode);
        st = partialTimespanNode.end - period;
      } else {
        final Node<T> partialTimespanNode = partialTimespans.getNextPartialTimespanNode(st);
        childNodes.add(partialTimespanNode);
        st = partialTimespanNode.end;
      }
    }


    while (st < end) {
      elem = null;
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
      //System.out.println("PARTIAL ST: " + st);
      if (elem == null) {
        if (st < startTime) {
          final Node<T> partialTimespanNode = partialTimespans.getNextPartialTimespanNode(st + period);
          //System.out.println("st < startTime: " + st);
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
