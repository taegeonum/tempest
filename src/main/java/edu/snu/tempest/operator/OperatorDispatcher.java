package edu.snu.tempest.operator;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by taegeonum on 9/14/15.
 */
public final class OperatorDispatcher<I, K, V> implements OutputEmitter<I> {

  private final ExecutorService executors;
  private final Operator<I, V> nextOperator;

  public OperatorDispatcher(final Operator<I, V> nextOperator,
                            final int numThreads) {
    this.executors = Executors.newFixedThreadPool(numThreads);
    this.nextOperator = nextOperator;
  }

  @Override
  public void emit(final I output) {

    executors.submit(new Runnable() {
      @Override
      public void run() {
        nextOperator.execute(output);
      }
    });
  }

  @Override
  public void close() throws Exception {
    executors.shutdown();
  }
}
