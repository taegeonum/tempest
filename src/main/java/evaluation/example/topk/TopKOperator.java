package evaluation.example.topk;

import edu.snu.tempest.operator.OutputEmitter;
import edu.snu.tempest.operator.window.timescale.TimeWindowOutputHandler;
import edu.snu.tempest.operator.window.timescale.TimescaleWindowOutput;
import edu.snu.tempest.operator.window.timescale.impl.DepOutputAndResult;

import javax.inject.Inject;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Logger;
import java.util.logging.Level;

public final class TopKOperator implements TimeWindowOutputHandler<Map<String, Long>, Map<String, Long>> {
  private static final Logger LOG = Logger.getLogger(TopKOperator.class.getName());

  private OutputEmitter<TimescaleWindowOutput<Map<String, Long>>> emitter;

  @Inject
  public TopKOperator() {

  }

  @Override
  public void execute(final TimescaleWindowOutput<Map<String, Long>> val) {
    // find top-k
    final Map<String, Long> output = val.output.result;
    final TreeMap<String, Long> sortedMap = new TreeMap<>(new ValueComparator(output));
    sortedMap.putAll(output);

    final Map<String, Long> topk = new HashMap<>();
    for (int i = 0; i < 10; i++) {
      final Map.Entry<String, Long> entry = sortedMap.pollFirstEntry();
      topk.put(entry.getKey(), entry.getValue());
    }

    LOG.log(Level.INFO, topk.toString());
    emitter.emit(new TimescaleWindowOutput<Map<String, Long>>(
        val.timescale, new DepOutputAndResult<Map<String, Long>>(val.output.numDepOutputs, topk)
        , val.startTime, val.endTime, val.fullyProcessed));
  }

  @Override
  public void prepare(final OutputEmitter<TimescaleWindowOutput<Map<String, Long>>> outputEmitter) {
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
