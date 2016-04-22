package vldb.operator.window.timescale.pafas;

import vldb.operator.OutputEmitter;
import vldb.operator.window.timescale.TimeWindowOutputHandler;
import vldb.operator.window.timescale.TimescaleWindowOutput;

import java.util.concurrent.CountDownLatch;

public final class LoggingHandler<I, O> implements TimeWindowOutputHandler<I, O> {

  private final String id;
  private CountDownLatch countDownLatch;

  public LoggingHandler(final String id, final CountDownLatch countDownLatch) {
    this.id = id;
    this.countDownLatch = countDownLatch;
  }

  @Override
  public void execute(final TimescaleWindowOutput<I> val) {
    System.out.println(id + " ts: " + val.timescale +
        ", timespan: [" + val.startTime + ", " + val.endTime + ")"
        + ", output: " + val.output.result);
    countDownLatch.countDown();
  }

  @Override
  public void prepare(final OutputEmitter<TimescaleWindowOutput<O>> outputEmitter) {

  }
}
