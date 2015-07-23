package org.edu.snu.tempest.examples.storm;

import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.edu.snu.tempest.operators.Timescale;
import org.edu.snu.tempest.operators.common.Aggregator;
import org.edu.snu.tempest.operators.common.WindowOutput;
import org.edu.snu.tempest.operators.dynamicmts.DynamicMTSOperator;
import org.edu.snu.tempest.operators.dynamicmts.impl.DynamicMTSOperatorImpl;
import org.edu.snu.tempest.operators.dynamicmts.signal.MTSSignalSender;
import org.edu.snu.tempest.operators.dynamicmts.signal.impl.ZkMTSParameters;
import org.edu.snu.tempest.operators.dynamicmts.signal.impl.ZkSignalSender;
import org.edu.snu.tempest.operators.staticmts.MTSOperator;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Creates DynamicMTSOperatorImpl and adds timescales by using ZkSignalSender.
 * 
 */
public final class MTSOperatorWithZookeeperExample {

  private MTSOperatorWithZookeeperExample() {

  }
  
  public static void main(String[] args) throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(1);
    Timescale ts = new Timescale(5, 3);
    List<Timescale> list = new LinkedList<>();
    list.add(ts);
    final long startTime = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime());
    Aggregator<Integer, Integer> testAggregator = new TestAggregator();

    // configure zookeeper setting.
    String identifier = "mts-test";
    String namespace = "mts";
    String address = "localhost:2181";
    
    JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.bindNamedParameter(ZkMTSParameters.OperatorIdentifier.class, identifier);
    cb.bindNamedParameter(ZkMTSParameters.ZkMTSNamespace.class, namespace);
    cb.bindNamedParameter(ZkMTSParameters.ZkServerAddress.class, address);
    
    Injector ij = Tang.Factory.getTang().newInjector(cb.build());
    ij.bindVolatileInstance(Aggregator.class, testAggregator);
    ij.bindVolatileInstance(List.class, list);
    ij.bindVolatileInstance(MTSOperator.OutputHandler.class, new TestHandler());
    ij.bindVolatileInstance(Long.class, startTime);
    final DynamicMTSOperator<Integer> operator = ij.getInstance(DynamicMTSOperatorImpl.class);
    MTSSignalSender sender = ij.getInstance(ZkSignalSender.class);

    operator.start();
    executor.submit(new Runnable() {
      @Override
      public void run() {
        Random rand = new Random();
        for (int i = 0; i < 3000; i++) {
          operator.execute(Math.abs(rand.nextInt() % 5));
          try {
            Thread.sleep(10);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }
    });

    // add timescales to the MTS operator
    Thread.sleep(1500);
    sender.addTimescale(new Timescale(10, 5));
    Thread.sleep(1000);
    sender.addTimescale(new Timescale(20, 8));
    Thread.sleep(1000);
    sender.addTimescale(new Timescale(15, 7));
    
    executor.shutdown();
    executor.awaitTermination(30, TimeUnit.SECONDS);
    operator.close();
    sender.close();
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
}
