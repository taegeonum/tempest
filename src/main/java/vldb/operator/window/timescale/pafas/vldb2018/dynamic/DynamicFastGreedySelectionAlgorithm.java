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

public class DynamicFastGreedySelectionAlgorithm<T> implements DependencyGraph.SelectionAlgorithm<T> {

  private final PartialTimespans<T> partialTimespans;
  private final OutputLookupTable<Node<T>> finalTimespans;
  private final long period;
  private final long startTime;

  @Inject
  private DynamicFastGreedySelectionAlgorithm(final DynamicPartialTimespans<T> partialTimespans,
                                              final OutputLookupTable<Node<T>> finalTimespans,
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
}
