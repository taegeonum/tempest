package vldb.operator.window.timescale.profiler;

import javax.inject.Inject;

public final class GraphBuildCostCounter {

  private long count;

  @Inject
  private GraphBuildCostCounter() {
    this.count = 0;
  }

  public long getCost() {
    return count;
  }

  public void addNode() {
    count += 1;
  }

  public void findEdge() {
    count += 1;
  }
}
