package vldb.operator.window.timescale.common;

import org.apache.reef.tang.annotations.Parameter;
import vldb.operator.window.timescale.parameter.NumThreads;

import javax.inject.Inject;
import java.util.concurrent.ForkJoinPool;

public final class SharedForkJoinPool {

  private final ForkJoinPool forkJoinPool;

  @Inject
  private SharedForkJoinPool(@Parameter(NumThreads.class) int numThreads) {
    this.forkJoinPool = new ForkJoinPool(numThreads);
  }

  public ForkJoinPool getForkJoinPool() {
    return forkJoinPool;
  }
}
