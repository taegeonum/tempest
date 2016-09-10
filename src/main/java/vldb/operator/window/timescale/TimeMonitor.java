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
  public long groupingTime = 0;
  public long nodeAdditionTime = 0;
  public long edgeAdditionTime = 0;

  @Inject
  private TimeMonitor() {

  }

  @Override
  public String toString() {
    final long pt = TimeUnit.NANOSECONDS.toMillis(partialTime);
    final long ft = TimeUnit.NANOSECONDS.toMillis(finalTime);
    final long ct = TimeUnit.NANOSECONDS.toMillis(continuousTime);
    final long gt = TimeUnit.NANOSECONDS.toMillis(groupingTime);
    final long nat = TimeUnit.NANOSECONDS.toMillis(nodeAdditionTime);
    final long eat = TimeUnit.NANOSECONDS.toMillis(edgeAdditionTime);

    return "PT\t" + pt +
        "\tFT\t" + ft +
        "\tBT\t" + ct +
        "\tCT\t" + (pt+ft) +
        "\tTT\t" + (pt+ft+ct) +
        "\tGT\t" + gt +
        "\tNAT\t" + nat +
        "\tEAT\t" + eat;
  }
}
