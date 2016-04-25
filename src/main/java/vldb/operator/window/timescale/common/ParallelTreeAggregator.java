package vldb.operator.window.timescale.common;


import vldb.operator.window.aggregator.CAAggregator;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;
import java.util.logging.Logger;

/**
 * This class does parallel tree aggregation for final aggregation.
 */
public final class ParallelTreeAggregator<I, T> implements AutoCloseable {
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
   * The minimum size which triggers parallel aggregation.
   */
  private final int sequentialThreshold;

  /**
   * A fork join pool executing parallel aggregation.
   */
  private final ForkJoinPool pool;

  /**
   * ParallelTreeAggregator for final aggregation.
   * @param numOfParallelThreads the number of threads
   * @param sequentialThreshold the minimum size which triggers parallel aggregation
   * @param finalAggregator final aggregator
   */
  public ParallelTreeAggregator(final int numOfParallelThreads,
                                final int sequentialThreshold,
                                final CAAggregator<I, T> finalAggregator) {
    this(numOfParallelThreads, sequentialThreshold, finalAggregator, new ForkJoinPool(numOfParallelThreads));
  }

  public ParallelTreeAggregator(final int numOfParallelThreads,
                                final int sequentialThreshold,
                                final CAAggregator<I, T> finalAggregator,
                                final ForkJoinPool pool) {
    this.sequentialThreshold = sequentialThreshold;
    this.finalAggregator = finalAggregator;
    this.pool = pool;
    this.numOfParallelThreads = numOfParallelThreads;
  }

  /**
   * This uses multiple threads to aggregate the dependent outputs and returns result.
   * @param dependentOutputs the dependent outputs
   * @return final result
   */
  public T doParallelAggregation(final List<T> dependentOutputs) {
    // aggregates dependent outputs
    //final ForkJoinPool pool = new ForkJoinPool(numOfParallelThreads);

    final T finalResult;
    // do parallel two-level tree aggregation if dependent size is large enough.
    if (dependentOutputs.size() >= sequentialThreshold) {
      finalResult = pool.invoke(new Aggregate(dependentOutputs, 0, dependentOutputs.size()));
    } else {
      try {
        finalResult = pool.submit(new Callable<T>() {
          @Override
          public T call() throws Exception {
            return finalAggregator.aggregate(dependentOutputs);
          }
        }).get();
      } catch (final InterruptedException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      } catch (final ExecutionException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }
    //pool.shutdown();
    return finalResult;
  }

  @Override
  public void close() throws Exception {
    pool.shutdown();
  }

  final class Aggregate extends RecursiveTask<T> {
    /**
     * Dependent outputs.
     */
    private final List<T> list;

    /**
     * Start index.
     */
    private final int start;

    /**
     * End index.
     */
    private final int end;

    /**
     * RecursiveTask for parallel aggregation.
     * @param list dependent outputs
     * @param start start index
     * @param end end index
     */
    Aggregate(final List<T> list, final int start, final int end) {
      this.list = list;
      this.start = start;
      this.end = end;
    }

    @Override
    protected T compute() {
      if (end - start == list.size()) {
        // root node
        final List<ForkJoinTask<T>> tasks = new LinkedList<>();
        final int hop = list.size() / numOfParallelThreads;
        // splits the dependent outputs and uses multiple threads for the aggregation of split outputs.
        for (int i = 0; i < numOfParallelThreads; i++) {
          final int startIndex = i * hop;
          final int endIndex = i == (numOfParallelThreads - 1) ? list.size() : startIndex + hop;
          final RecursiveTask<T> task = new Aggregate(list, startIndex, endIndex);
          tasks.add(task.fork());
        }
        // wait until all of the aggregation is finished.
        final List<T> finalList = new LinkedList<>();
        for (ForkJoinTask<T> task : tasks) {
          finalList.add(task.join());
        }
        // do tree root aggregation in single thread.
        return finalAggregator.aggregate(finalList);
      } else {
        // child nodes
        final List<T> splited = list.subList(start, end);
        final T finalResult = finalAggregator.aggregate(splited);
        return finalResult;
      }
    }
  }
}
