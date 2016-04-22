package vldb.operator.window.timescale.pafas;

import org.apache.reef.tang.annotations.Parameter;
import vldb.operator.window.timescale.common.OutputLookupTable;
import vldb.operator.window.timescale.common.WindowTimeAndOutput;
import vldb.operator.window.timescale.parameter.StartTime;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;

public class OntheflySelectionAlgorithm<T> implements DependencyGraph.SelectionAlgorithm<T> {

  private final PartialTimespans<T> partialTimespans;
  private final OutputLookupTable<Node<T>> finalTimespans;
  private final long period;
  private final long startTime;

  @Inject
  private OntheflySelectionAlgorithm(final PartialTimespans<T> partialTimespans,
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
    // Just select from partial timespans
    long st = start;
    while (st < end) {
      WindowTimeAndOutput<Node<T>> elem = null;
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
    //System.out.println("[" + st + ", " + end + "): " + childNodes);
    return childNodes;
  }
}
