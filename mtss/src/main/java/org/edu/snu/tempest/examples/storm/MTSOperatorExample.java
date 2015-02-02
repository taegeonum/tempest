package org.edu.snu.tempest.examples.storm;

import org.edu.snu.tempest.Timescale;
import org.edu.snu.tempest.operator.MTSOperator;
import org.edu.snu.tempest.operator.MTSOperator.Aggregator;
import org.edu.snu.tempest.operator.MTSOperator.OutputHandler;
import org.edu.snu.tempest.operator.WindowOutput;
import org.edu.snu.tempest.operator.impl.DynamicMTSOperatorImpl;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;

/*
 * Example
 * Creates a DefaultMTSOperatorImpl 
 * and aggregates Input
 */
public class MTSOperatorExample {
  
  public static void main(String[] args) throws Exception {
    Timescale ts = new Timescale(5, 3);
    Aggregator<Integer, Integer> testAggregator = new TestAggregator();
    List<Timescale> list = new LinkedList<>();
    list.add(ts);
    MTSOperator<Integer, Integer> operator = new DynamicMTSOperatorImpl<Integer, Integer>(testAggregator, list, new TestHandler());
    operator.start();
    
    Random rand = new Random();
    for (int i = 0; i < 1500; i++) {
      operator.execute(Math.abs(rand.nextInt() % 5));
      
      if (i == 200) {
        operator.onTimescaleAddition(new Timescale(10, 2));
      }
      
      if (i == 500) {
        operator.onTimescaleAddition(new Timescale(7, 4));
      }
      Thread.sleep(10);
    }
    
    operator.close();
  }
  
  static class TestAggregator implements Aggregator<Integer, Integer> {

    public TestAggregator() {
      
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
  
  static class TestHandler implements OutputHandler<Integer> {

    @Override
    public void onNext(WindowOutput<Integer> arg0) {
      System.out.println(arg0);
    }
  }
}
