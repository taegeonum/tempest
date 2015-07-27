package edu.snu.tempest.utils;


import edu.snu.tempest.operators.Timescale;
import edu.snu.tempest.operators.common.MTSWindowOutput;
import edu.snu.tempest.operators.staticmts.MTSOperator;

import java.util.Map;
import java.util.Queue;

public final class TestOutputHandler implements MTSOperator.MTSOutputHandler<Map<Integer, Long>> {
  private final Map<Timescale,
      Queue<MTSWindowOutput<Map<Integer, Long>>>> results;
  private final long startTime;
  private final Monitor monitor;
  private int count = 0;

  public TestOutputHandler(final Monitor monitor,
                           final Map<Timescale, Queue<MTSWindowOutput<Map<Integer, Long>>>> results,
                           final long startTime) {
    this.monitor = monitor;
    this.results = results;
    this.startTime = startTime;
  }

  @Override
  public void onNext(final MTSWindowOutput<Map<Integer, Long>> windowOutput) {
    if (count < 2) {
      if (windowOutput.fullyProcessed) {
        Queue<MTSWindowOutput<Map<Integer, Long>>> outputs = this.results.get(windowOutput.timescale);
        System.out.println(windowOutput);
        outputs.add(windowOutput);
      }
    } else {
      this.monitor.mnotify();
    }

    if (windowOutput.timescale.windowSize == 8) {
      count++;
    }
  }
}