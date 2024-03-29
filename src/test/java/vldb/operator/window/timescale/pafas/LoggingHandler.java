package vldb.operator.window.timescale.pafas;

import vldb.operator.OutputEmitter;
import vldb.operator.window.timescale.TimeWindowOutputHandler;
import vldb.operator.window.timescale.TimescaleWindowOutput;

public final class LoggingHandler<I, O> implements TimeWindowOutputHandler<I, O> {

  private final String id;

  public LoggingHandler(final String id) {
    this.id = id;
  }

  @Override
  public void execute(final TimescaleWindowOutput<I> val) {
    System.out.println(id + " ts: " + val.timescale +
        ", timespan: [" + val.startTime + ", " + val.endTime + ")"
        + ", output: " + val.output.result);
  }

  @Override
  public void prepare(final OutputEmitter<TimescaleWindowOutput<O>> outputEmitter) {

  }
}
