package evaluation.example.topk;

import java.util.List;
import java.util.Map;

public final class TopkOutput {

  public final List<Map.Entry<String, Long>> topk;
  public final long count;

  public TopkOutput(final long count,
                    final List<Map.Entry<String, Long>> topk) {
    this.count = count;
    this.topk = topk;
  }
}
