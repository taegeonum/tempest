package vldb.evaluation;

import vldb.operator.window.timescale.TimeMonitor;

import javax.inject.Inject;

/**
 * Created by taegeonum on 8/26/17.
 */
public final class Metrics {
  public final static String PARTIAL_COUNT = "PARTIAL_COUNT";
  public final static String FINAL_COUNT = "FINAL_COUNT";

  public long partialCount;
  public long finalCount;

  public long elapsedTime;
  public final TimeMonitor timeMonitor;

  public long storedPartial;

  public long storedFinal;

  public long storedInter;
  @Inject
  public Metrics(final TimeMonitor timeMonitor) {
    this.partialCount = 0;
    this.finalCount = 0;
    this.elapsedTime = 0;
    this.storedFinal = 0;
    this.storedPartial = 0;
    this.storedInter = 0;
    this.timeMonitor = timeMonitor;
  }

  public void incrementPartial() {
    partialCount += 1;
  }

  public void incrementFinal() {
    finalCount += 1;
  }

  public void setElapsedTime(final long et) {
    elapsedTime = et;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append(partialCount);
    sb.append("\t");
    sb.append(finalCount);
    sb.append("\t");
    sb.append(elapsedTime);
    sb.append("\t");
    sb.append(timeMonitor);

    return sb.toString();
  }
}
