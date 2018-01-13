package vldb.operator.window.timescale.pafas.vldb2018.dynamic;

import org.apache.reef.tang.annotations.Parameter;
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

public class DynamicOntheFlySelectionAlgorithm<T> implements DependencyGraph.SelectionAlgorithm<T> {

  private final PartialTimespans<T> partialTimespans;
  private final long period;
  private final long startTime;

  @Inject
  private DynamicOntheFlySelectionAlgorithm(final DynamicPartialTimespans<T> partialTimespans,
                                            final PeriodCalculator periodCalculator,
                                            @Parameter(StartTime.class) long startTime) {
    this.partialTimespans = partialTimespans;
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
      final Node<T> partialTimespanNode = partialTimespans.getNextPartialTimespanNode(st);
      //System.out.println("start: " + start + ", " + end + ", st: " + st + ", " + partialTimespans);
      childNodes.add(partialTimespanNode);
      st = partialTimespanNode.end;
    }
    //System.out.println("[" + st + ", " + end + "): " + childNodes);
    return childNodes;
  }
}
