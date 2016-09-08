package vldb.operator.window.timescale.common;

import org.apache.reef.tang.annotations.Parameter;
import vldb.operator.window.timescale.parameter.NumThreads;

import javax.inject.Inject;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class SharedWorkStealingPool {

  private final ExecutorService workStealingPool;

  @Inject
  private SharedWorkStealingPool(@Parameter(NumThreads.class) int numThreads) {
    this.workStealingPool = Executors.newWorkStealingPool(numThreads);
  }

  public ExecutorService getWorkStealingPool() {
    return workStealingPool;
  }
}
