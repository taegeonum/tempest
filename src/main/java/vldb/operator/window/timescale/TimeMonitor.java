package vldb.operator.window.timescale;

import javax.inject.Inject;
import java.util.concurrent.TimeUnit;

/**
 * Created by taegeonum on 8/23/16.
 */
public final class TimeMonitor {

  public long finalTime = 0;
  public long partialTime = 0;
  public long continuousTime = 0;
  public long storedKey = 0;

  @Inject
  private TimeMonitor() {

  }

  @Override
  public String toString() {
    final long pt = TimeUnit.NANOSECONDS.toMillis(partialTime);
    final long ft = TimeUnit.NANOSECONDS.toMillis(finalTime);
    final long ct = TimeUnit.NANOSECONDS.toMillis(continuousTime);

    return "\t" + pt +
        "\t" + ft +
        "\t" + ct +
        "\t" + (pt+ft) +
        "\t" + (pt+ft+ct);
  }
}
