package edu.snu.tempest.examples.utils;

import edu.snu.tempest.operators.staticmts.MTSOperator;
import edu.snu.tempest.operators.common.MTSWindowOutput;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Default OutputHandler.
 */
public final class DefaultOutputHandler<V> implements MTSOperator.MTSOutputHandler<V> {
  
  private static final Logger LOG = Logger.getLogger(DefaultOutputHandler.class.getName());
  
  @Inject
  public DefaultOutputHandler() {
    
  }
  
  @Override
  public void onNext(final MTSWindowOutput<V> output) {
    LOG.log(Level.INFO, output.toString());
  }

}
