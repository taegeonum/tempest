package org.edu.snu.tempest.operators.dynamicmts.impl;


import junit.framework.Assert;
import org.edu.snu.tempest.operators.Timescale;
import org.edu.snu.tempest.operators.common.WindowOutput;
import org.edu.snu.tempest.operators.common.aggregators.CountByKeyAggregator;
import org.edu.snu.tempest.operators.dynamicmts.DynamicMTSOperator;
import org.edu.snu.tempest.operators.dynamicmts.signal.MTSSignalReceiver;
import org.edu.snu.tempest.operators.dynamicmts.TimescaleSignalListener;
import org.edu.snu.tempest.operators.staticmts.MTSOperator;
import org.edu.snu.tempest.utils.MTSTestUtils;
import org.edu.snu.tempest.utils.Monitor;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class DynamicMTSOperatorTest {

  @Test
  public void multipleTimescaleAggregationTest() throws Exception {
    final ExecutorService executor = Executors.newFixedThreadPool(1);
    final Monitor monitor = new Monitor();
    final Boolean finished = new Boolean(false);
    final Random random = new Random();
    final List<Timescale> timescales = new LinkedList<>();
    final Timescale ts1 = new Timescale(2, 2);
    final Timescale ts2 = new Timescale(4, 4);
    final Timescale ts3 = new Timescale(8, 8);
    timescales.add(ts1);
    timescales.add(ts2);
    timescales.add(ts3);
    final long startTime = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime());
    final Map<Timescale, Queue<WindowOutput<Map<Integer, Long>>>> results = new HashMap<>();
    results.put(ts1, new LinkedList<WindowOutput<Map<Integer, Long>>>());
    results.put(ts2, new LinkedList<WindowOutput<Map<Integer, Long>>>());
    results.put(ts3, new LinkedList<WindowOutput<Map<Integer, Long>>>());

    final DynamicMTSOperator<Integer> operator =
        new DynamicMTSOperatorImpl<>(new CountByKeyAggregator<Integer, Integer>(new IntegerExtractor()),
            timescales, new TestHandler(monitor, results, startTime), new TestSignalReceiver(), 1, startTime);
    operator.start();

    executor.submit(new Runnable() {
      @Override
      public void run() {
        while (!finished.booleanValue()) {
          operator.execute(Math.abs(random.nextInt() % 10));
          try {
            Thread.sleep(10);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }
    });

    monitor.mwait();
    operator.close();

    // check outputs
    while (!results.get(ts3).isEmpty()) {
      final WindowOutput<Map<Integer, Long>> ts3Output = results.get(ts3).poll();
      final WindowOutput<Map<Integer, Long>> ts2Output1 = results.get(ts2).poll();
      final WindowOutput<Map<Integer, Long>> ts2Output2 = results.get(ts2).poll();
      Assert.assertEquals(MTSTestUtils.merge(ts2Output1.output, ts2Output2.output), ts3Output.output);

      final WindowOutput<Map<Integer, Long>> ts1Output1 = results.get(ts1).poll();
      final WindowOutput<Map<Integer, Long>> ts1Output2 = results.get(ts1).poll();
      Assert.assertEquals(MTSTestUtils.merge(ts1Output1.output, ts1Output2.output), ts2Output1.output);

      final WindowOutput<Map<Integer, Long>> ts1Output3 = results.get(ts1).poll();
      final WindowOutput<Map<Integer, Long>> ts1Output4 = results.get(ts1).poll();
      Assert.assertEquals(MTSTestUtils.merge(ts1Output3.output, ts1Output4.output), ts2Output2.output);
    }
  }

  class IntegerExtractor implements CountByKeyAggregator.KeyExtractor<Integer, Integer> {
    @Override
    public Integer getKey(final Integer value) {
      return value;
    }
  }

  class TestHandler implements MTSOperator.OutputHandler<Map<Integer, Long>> {
    private final Map<Timescale,
        Queue<WindowOutput<Map<Integer, Long>>>> results;
    private final long startTime;
    private final Monitor monitor;
    private int count = 0;

    public TestHandler(final Monitor monitor,
                       final Map<Timescale,
                           Queue<WindowOutput<Map<Integer, Long>>>> results,
                       final long startTime) {
      this.monitor = monitor;
      this.results = results;
      this.startTime = startTime;
    }

    @Override
    public void onNext(final WindowOutput<Map<Integer, Long>> windowOutput) {
      if (count < 2) {
        if (windowOutput.fullyProcessed) {
          Queue<WindowOutput<Map<Integer, Long>>> outputs = this.results.get(windowOutput.timescale);
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

  class TestSignalReceiver implements MTSSignalReceiver {
    private TimescaleSignalListener listener;

    @Override
    public void start() throws Exception {

    }

    @Override
    public void addTimescaleSignalListener(final TimescaleSignalListener listener) {
      this.listener = listener;
    }

    @Override
    public void close() throws Exception {

    }
  }
}
