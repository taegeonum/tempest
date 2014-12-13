package edu.snu.org.mtss;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.impl.ThreadPoolStage;
import org.junit.Test;

import edu.snu.org.util.ReduceByKeyTuple;
import edu.snu.org.util.ReduceFunc;
import edu.snu.org.util.Timescale;

public class MTSOperatorTest {
  
  
  @Test
  public void createTest() throws Exception {
    
    ThreadPoolStage<Collection<MTSOutput<Map<Integer, Integer>>>> executor = new ThreadPoolStage<>("stage", 
        new EventHandler<Collection<MTSOutput<Map<Integer, Integer>>>>() {
      @Override
      public void onNext(Collection<MTSOutput<Map<Integer ,Integer>>> data) {
        for (MTSOutput<Map<Integer, Integer>> d : data) {
          System.out.println(d);
        }
      }
    }, 1);
    

    List<Timescale> timescales = new LinkedList<>();
    //timescales.add(new Timescale(2, 2, TimeUnit.SECONDS, TimeUnit.SECONDS));
    //timescales.add(new Timescale(4, 2, TimeUnit.SECONDS, TimeUnit.SECONDS));
    //timescales.add(new Timescale(8, 4, TimeUnit.SECONDS, TimeUnit.SECONDS));

    timescales.add(new Timescale(4, 2, TimeUnit.SECONDS, TimeUnit.SECONDS));
    timescales.add(new Timescale(6, 4, TimeUnit.SECONDS, TimeUnit.SECONDS));

    ReduceByKeyComputation<Integer, Integer> computation = new ReduceByKeyComputation<>(new ReduceFunc<Integer>() {
      @Override
      public Integer compute(Integer value, Integer sofar) {
        return value + sofar;
      }
    });

    
    ReduceByKeyMTSOperator<Integer, Integer> operator = new ReduceByKeyMTSOperator<>(timescales, computation);
    
    System.out.println(operator);
    operator.receiveInput(new ReduceByKeyTuple<Integer, Integer>(1,3));
    executor.onNext(operator.flush(2L));
    System.out.println(operator);

    operator.receiveInput(new ReduceByKeyTuple<Integer, Integer>(1,4));
    executor.onNext(operator.flush(4L));
    System.out.println(operator);

    operator.receiveInput(new ReduceByKeyTuple<Integer, Integer>(1,5));
    executor.onNext(operator.flush(6L));
    System.out.println(operator);
    
    operator.receiveInput(new ReduceByKeyTuple<Integer, Integer>(1,6));
    executor.onNext(operator.flush(8L));
    System.out.println(operator);
    
    operator.receiveInput(new ReduceByKeyTuple<Integer, Integer>(1,7));
    executor.onNext(operator.flush(10L));
    System.out.println(operator);

    operator.receiveInput(new ReduceByKeyTuple<Integer, Integer>(1,8));
    executor.onNext(operator.flush(12L));
    System.out.println(operator);

    
    operator.receiveInput(new ReduceByKeyTuple<Integer, Integer>(1,9));
    executor.onNext(operator.flush(14L));
    System.out.println(operator);
    
    operator.receiveInput(new ReduceByKeyTuple<Integer, Integer>(1,10));
    operator.receiveInput(new ReduceByKeyTuple<Integer, Integer>(2,2));
    operator.receiveInput(new ReduceByKeyTuple<Integer, Integer>(3,3));
    executor.onNext(operator.flush(16L));
    
    operator.receiveInput(new ReduceByKeyTuple<Integer, Integer>(1,10));
    operator.receiveInput(new ReduceByKeyTuple<Integer, Integer>(2,2));
    operator.receiveInput(new ReduceByKeyTuple<Integer, Integer>(3,3));
    executor.onNext(operator.flush(18L));
    
    operator.receiveInput(new ReduceByKeyTuple<Integer, Integer>(1,10));
    operator.receiveInput(new ReduceByKeyTuple<Integer, Integer>(2,2));
    operator.receiveInput(new ReduceByKeyTuple<Integer, Integer>(3,3));
    executor.onNext(operator.flush(20L));
    
    operator.receiveInput(new ReduceByKeyTuple<Integer, Integer>(1,10));
    operator.receiveInput(new ReduceByKeyTuple<Integer, Integer>(2,2));
    operator.receiveInput(new ReduceByKeyTuple<Integer, Integer>(3,3));
    executor.onNext(operator.flush(22L));
    
    
    operator.receiveInput(new ReduceByKeyTuple<Integer, Integer>(1,10));
    operator.receiveInput(new ReduceByKeyTuple<Integer, Integer>(2,2));
    operator.receiveInput(new ReduceByKeyTuple<Integer, Integer>(3,3));
    executor.onNext(operator.flush(24L));
    System.out.println(operator);
  }
}
