package edu.snu.org.mtss;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.reef.wake.EventHandler;
import org.junit.Test;

import edu.snu.org.util.ReduceFunc;
import edu.snu.org.util.Timescale;

public class MTSOperatorTest {
  
  
  @Test
  public void createTest() throws Exception {
    
    List<Timescale> timescales = new LinkedList<>();
    //timescales.add(new Timescale(2, 2, TimeUnit.SECONDS, TimeUnit.SECONDS));
    //timescales.add(new Timescale(4, 2, TimeUnit.SECONDS, TimeUnit.SECONDS));
    //timescales.add(new Timescale(8, 4, TimeUnit.SECONDS, TimeUnit.SECONDS));

    timescales.add(new Timescale(4, 2, TimeUnit.SECONDS, TimeUnit.SECONDS));
    timescales.add(new Timescale(6, 4, TimeUnit.SECONDS, TimeUnit.SECONDS));

    MTSOperator<Integer, Integer> operator = new MTSOperator<Integer, Integer>(timescales, new ReduceFunc<Integer>() {
      @Override
      public Integer compute(Integer value, Integer sofar) {
        return value + sofar;
      }
    }, new EventHandler<MTSOutput<Integer, Integer>>() {
      @Override
      public void onNext(MTSOutput<Integer ,Integer> data) {
        System.out.println(data);
      }
    });
    
    
    
    
    System.out.println(operator);
    operator.addData(1, 3);
    operator.flush(2L);
    System.out.println(operator);

    operator.addData(1, 4);
    operator.flush(4L);
    System.out.println(operator);

    operator.addData(1, 5);
    operator.flush(6L);
    System.out.println(operator);
    
    operator.addData(1, 6);
    operator.flush(8L);
    System.out.println(operator);
    
    operator.addData(1, 7);
    operator.flush(10L);
    System.out.println(operator);

    operator.addData(1, 8);
    operator.flush(12L);
    System.out.println(operator);

    
    operator.addData(1, 9);
    operator.flush(14L);
    System.out.println(operator);
    
    operator.addData(1, 10);
    operator.flush(16L);
    System.out.println(operator);
  }
}
