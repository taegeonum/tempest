package vldb.operator.window.timescale.pafas.dynamic;

import org.apache.reef.tang.annotations.Parameter;
import vldb.operator.window.timescale.Timescale;
import vldb.operator.window.timescale.common.TimescaleParser;
import vldb.operator.window.timescale.parameter.StartTime;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by taegeonum on 8/16/16.
 */
public final class WindowManager {

  public final List<Timescale> timescales;
  private final Map<Timescale, Long> startTimeMap;

  @Inject
  private WindowManager(final TimescaleParser tsParser,
                        @Parameter(StartTime.class) final long startTime) {
    this.timescales = tsParser.timescales;
    this.startTimeMap = new HashMap<>();
    for (final Timescale ts : timescales) {
      startTimeMap.put(ts, startTime);
    }
  }

  public long timescaleStartTime(final Timescale ts) {
    return startTimeMap.get(ts);
  }

  public void addWindow(final Timescale ts, final long time) {
    final Iterator<Timescale> iterator = timescales.iterator();
    int index = 0;
    boolean added = false;
    while (iterator.hasNext()) {
      final Timescale tts = iterator.next();
      if (tts.windowSize > ts.windowSize) {
        timescales.add(index, ts);
        added = true;
        break;
      } else {
        index += 1;
      }
    }

    if (!added) {
      timescales.add(ts);
    }
    startTimeMap.put(ts, time);
  }

  public void removeWindow(final Timescale ts, final long time) {
    startTimeMap.remove(ts);
    timescales.remove(ts);
  }

  public long getRebuildSize() {
    if (timescales.size() == 1) {
      return timescales.get(0).intervalSize;
    }
    return timescales.get(timescales.size()-1).windowSize - timescales.get(0).windowSize;
  }
}
