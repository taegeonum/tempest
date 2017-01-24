package atc.operator.window.timescale.profiler;

import junit.framework.Assert;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Test;
import atc.evaluation.parameter.EndTime;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public final class AggregationCounterTest {

  @Test
  public void testAggregationCounter() throws InjectionException, ExecutionException, InterruptedException {
    final int numThreads = 20;
    final int numInput = 5000;
    final ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
    final List<Future> futureTasks = new LinkedList<>();

    final Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileParameter(EndTime.class, 10L);
    final AggregationCounter aggregationCounter = injector.getInstance(AggregationCounter.class);

    for (int i = 0; i < numThreads; i++) {
      futureTasks.add(executorService.submit(new Runnable() {
        @Override
        public void run() {
          for (int j = 0; j < numInput; j++) {
            aggregationCounter.incrementPartialAggregation();
          }
        }
      }));
    }

    while (!futureTasks.isEmpty()) {
      futureTasks.remove(0).get();
    }
    Assert.assertEquals(numThreads*numInput, aggregationCounter.getNumPartialAggregation());

    for (int i = 0; i < numThreads; i++) {
      futureTasks.add(executorService.submit(new Runnable() {
        @Override
        public void run() {
          for (int j = 0; j < numInput; j++) {
            aggregationCounter.incrementFinalAggregation();
          }
        }
      }));
    }

    while (!futureTasks.isEmpty()) {
      futureTasks.remove(0).get();
    }
    Assert.assertEquals(numThreads*numInput, aggregationCounter.getNumFinalAggregation());
  }
}
