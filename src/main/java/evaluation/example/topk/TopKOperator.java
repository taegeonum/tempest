package evaluation.example.topk;

import edu.snu.tempest.operator.OutputEmitter;
import edu.snu.tempest.operator.window.timescale.TimeWindowOutputHandler;
import edu.snu.tempest.operator.window.timescale.TimescaleWindowOutput;
import edu.snu.tempest.operator.window.timescale.impl.DepOutputAndResult;

import javax.inject.Inject;
import java.util.*;
import java.util.logging.Logger;

public final class TopKOperator implements
    TimeWindowOutputHandler<Map<String, Long>, List<Map.Entry<String, Long>>> {
  private static final Logger LOG = Logger.getLogger(TopKOperator.class.getName());

  private OutputEmitter<TimescaleWindowOutput<List<Map.Entry<String, Long>>>> emitter;

  @Inject
  public TopKOperator() {

  }

  @Override
  public void execute(final TimescaleWindowOutput<Map<String, Long>> val) {
    // find top-k
    final Map<String, Long> output = val.output.result;
    final List<Map.Entry<String, Long>> sorted = new LinkedList<>(output.entrySet());
    Collections.sort(sorted, new Comparator<Map.Entry<String, Long>>() {
      @Override
      public int compare(final Map.Entry<String, Long> o1, final Map.Entry<String, Long> o2) {
        return o2.getValue().compareTo(o1.getValue());
      }
    });

    final List<Map.Entry<String, Long>> topk = sorted.subList(0, 10);
    //LOG.log(Level.INFO, topk.toString());
    emitter.emit(new TimescaleWindowOutput<>(
        val.timescale, new DepOutputAndResult<>(val.output.numDepOutputs, topk)
        , val.startTime, val.endTime, val.fullyProcessed));
  }

  @Override
  public void prepare(final OutputEmitter<TimescaleWindowOutput<List<Map.Entry<String, Long>>>> outputEmitter) {
    emitter = outputEmitter;
  }

  class ValueComparator implements Comparator<String> {
    final Map<String, Long> base;

    public ValueComparator(final Map<String, Long> base) {
      this.base = base;
    }

    // Note: this comparator imposes orderings that are inconsistent with
    // equals.
    public int compare(String a, String b) {
      if (base.get(a) >= base.get(b)) {
        return 1;
      } else {
        return -1;
      }
    }
  }
}
