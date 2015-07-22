package org.edu.snu.tempest.operators.staticmts;

import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Stage;
import org.edu.snu.tempest.examples.utils.DefaultOutputHandler;
import org.edu.snu.tempest.operators.common.WindowOutput;

/**
 * Static MTS operator interface.
 * It receives multiple timescales at starting time
 * and produces multi-time scale outputs.
 */
public interface MTSOperator<I> extends Stage {

  /**
   * Start of MTSOperator.
   */
  void start();
  
  /**
   * It receives input from this function.
   * 
   * @param val input value
   */
  void execute(final I val);

  /**
   * MTSOperator sends window outputs to OutputHandler.
   */
  @DefaultImplementation(DefaultOutputHandler.class)
  public interface OutputHandler<V> extends EventHandler<WindowOutput<V>> {

  }
}
