package vldb.operator.window.timescale.common;

/**
 * Created by taegeonum on 9/16/15.
 */
public final class DepOutputAndResult<V> {

  public final int numDepOutputs;
  public final V result;

  public DepOutputAndResult(final int numDepOutputs,
                            final V result) {
    this.numDepOutputs = numDepOutputs;
    this.result = result;
  }
}
