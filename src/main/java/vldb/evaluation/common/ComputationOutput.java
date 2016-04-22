package vldb.evaluation.common;

/**
 * Created by taegeonum on 9/16/15.
 */
public final class ComputationOutput<V> {

  public final long computation;
  public final V output;
  public final int dependentOutputs;

  public ComputationOutput(final long computation,
                           final int dependentOutputs,
                           final V output) {
    this.computation = computation;
    this.output = output;
    this.dependentOutputs = dependentOutputs;
  }
}
