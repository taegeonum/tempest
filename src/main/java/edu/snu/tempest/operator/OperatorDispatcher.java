package edu.snu.tempest.operator;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by taegeonum on 9/14/15.
 */
public final class OperatorDispatcher<I, K, V> implements OutputEmitter<I> {

  private final ConcurrentMap<K, ExecutorService> executors;
  private final KeyExtractor<I, K> keyExtractor;
  private final Operator<I, V> nextOperator;

  public OperatorDispatcher(final Operator<I, V> nextOperator,
                            final KeyExtractor<I, K> keyExtractor) {
    this.executors = new ConcurrentHashMap<>();
    this.keyExtractor = keyExtractor;
    this.nextOperator = nextOperator;
  }

  @Override
  public void emit(final I output) {
    final K key = keyExtractor.getKey(output);
    ExecutorService executor = executors.get(keyExtractor.getKey(output));
    if (executor == null) {
      executors.putIfAbsent(key, Executors.newFixedThreadPool(1));
      executor = executors.get(key);
    }
    executor.submit(new Runnable() {
      @Override
      public void run() {
        nextOperator.execute(output);
      }
    });
  }

  @Override
  public void close() throws Exception {
    for (final ExecutorService executor : executors.values()) {
      executor.shutdown();
    }
  }

  public interface KeyExtractor<I, K> {
    K getKey(I output);
  }
}
