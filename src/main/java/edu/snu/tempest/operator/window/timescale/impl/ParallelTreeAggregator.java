package edu.snu.tempest.operator.window.timescale.impl;


import edu.snu.tempest.operator.window.aggregator.CAAggregator;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;
import java.util.logging.Logger;

/**
 * This class does parallel tree aggregation for final aggregation.
 */
final class ParallelTreeAggregator<I, T> {
  private static final Logger LOG = Logger.getLogger(ParallelTreeAggregator.class.getCanonicalName());

  /**
   * The number of threads for parallel aggregation.
   */
  private final int numOfParallelThreads;

  /**
   * Final aggregator.
   */
  private final CAAggregator<I, T> finalAggregator;

  /**
   * ParallelTreeAggregator for final aggregation.
   * @param numOfParallelThreads the number of threads
   * @param finalAggregator final aggregator
   */
  public ParallelTreeAggregator(final int numOfParallelThreads,
                                final CAAggregator<I, T> finalAggregator) {
    this.numOfParallelThreads = numOfParallelThreads;
    this.finalAggregator = finalAggregator;
  }

  /**
   * This uses multiple threads to aggregate the dependent outputs and returns result.
   * @param dependentOutputs the dependent outputs
   * @return final result
   */
  public T doParallelAggregation(final List<T> dependentOutputs) {
    // aggregates dependent outputs
    final ExecutorService executor = Executors.newFixedThreadPool(numOfParallelThreads);
    final T finalResult;
    // do parallel two-level tree aggregation if dependent size is large enough.
    if (dependentOutputs.size() >= numOfParallelThreads * 2) {
      final List<Future<T>> futures = new LinkedList<>();
      final int hop = dependentOutputs.size() / numOfParallelThreads;
      // splits the dependent outputs and uses multiple threads for the aggregation of split outputs.
      for (int i = 0; i < numOfParallelThreads; i++) {
        final int startIndex = i * hop;
        final int endIndex = i == (numOfParallelThreads - 1) ? dependentOutputs.size() : startIndex + hop;
        final List<T> splited = dependentOutputs.subList(startIndex, endIndex);
        futures.add(executor.submit(new Callable<T>() {
          @Override
          public T call() throws Exception {
            final T finalResult = finalAggregator.aggregate(splited);
            return finalResult;
          }
        }));
      }
      // wait until all of the aggregation is finished.
      final List<T> finalList = new LinkedList<>();
      for (Future<T> future : futures) {
        try {
          finalList.add(future.get());
        } catch (final InterruptedException e) {
          e.printStackTrace();
        } catch (final ExecutionException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      }
      // do tree root aggregation in single thread.
      finalResult = finalAggregator.aggregate(finalList);
    } else {
      finalResult = finalAggregator.aggregate(dependentOutputs);
    }
    executor.shutdown();
    return finalResult;
  }
}
