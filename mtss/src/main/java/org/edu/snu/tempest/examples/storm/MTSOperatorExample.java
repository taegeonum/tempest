package org.edu.snu.tempest.examples.storm;

import org.edu.snu.tempest.operators.Timescale;
import org.edu.snu.tempest.operators.common.Aggregator;
import org.edu.snu.tempest.operators.common.WindowOutput;
import org.edu.snu.tempest.operators.dynamicmts.DynamicMTSOperator;
import org.edu.snu.tempest.operators.dynamicmts.impl.DynamicMTSOperatorImpl;
import org.edu.snu.tempest.operators.dynamicmts.signal.MTSSignalReceiver;
import org.edu.snu.tempest.operators.dynamicmts.signal.TimescaleSignalListener;
import org.edu.snu.tempest.operators.staticmts.MTSOperator;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Dynamic MTSOperator Example.
 */
public final class MTSOperatorExample {

  private MTSOperatorExample() {

  }

  public static void main(String[] args) throws Exception {
    Timescale ts = new Timescale(5, 3);
    Aggregator<Integer, Integer> testAggregator = new TestAggregator();
    List<Timescale> list = new LinkedList<>();
    list.add(ts);
    final long startTime = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime());
    DynamicMTSOperator<Integer> operator =
        new DynamicMTSOperatorImpl<>(testAggregator, list,
           new TestHandler(), new Receiver(), startTime);

    operator.start();
    Random rand = new Random();
    for (int i = 0; i < 1500; i++) {
      operator.execute(Math.abs(rand.nextInt() % 5));
      
      if (i == 200) {
        operator.onTimescaleAddition(new Timescale(10, 2), startTime);
      }
      
      if (i == 500) {
        operator.onTimescaleAddition(new Timescale(7, 4), startTime);
      }
      Thread.sleep(10);
    }
    operator.close();
  }
  
  private static final class TestAggregator implements Aggregator<Integer, Integer> {
    private TestAggregator() {
      
    }
    
    @Override
    public Integer init() {
      return 0;
    }

    @Override
    public Integer partialAggregate(Integer oldVal, Integer newVal) {
      return oldVal + newVal;
    }

    @Override
    public Integer finalAggregate(List<Integer> partials) {
      int sum = 0;
      for (Integer partial : partials) {
        sum += partial;
      }
      return sum;
    }
  }
  
  private static final class TestHandler implements MTSOperator.OutputHandler<Integer> {
    private TestHandler() {

    }

    @Override
    public void onNext(WindowOutput<Integer> arg0) {
      System.out.println(arg0);
    }
  }

  private static final class Receiver implements MTSSignalReceiver {
    private Receiver() {

    }

    @Override
    public void start() throws Exception {

    }

    @Override
    public void addTimescaleSignalListener(TimescaleSignalListener listener) {

    }

    @Override
    public void close() throws Exception {

    }
  }
}
