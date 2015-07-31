package edu.snu.tempest.operator;

import org.apache.reef.wake.Stage;
import org.apache.reef.wake.EventHandler;

public interface WindowOperator<I> extends Stage {
  /**
   * Start of WindowOperator.
   */
  void start();

  /**
   * It receives input from this function.
   *
   * @param val input value
   */
  void execute(final I val);

  /**
   * Output handler for windowing.
   * @param <V> output
   */
  public interface WindowOutputHandler<V> extends EventHandler<V> {

  }
}
