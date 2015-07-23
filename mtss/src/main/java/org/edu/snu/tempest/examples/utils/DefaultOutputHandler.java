package org.edu.snu.tempest.examples.utils;

import org.edu.snu.tempest.operators.common.WindowOutput;
import org.edu.snu.tempest.operators.staticmts.MTSOperator;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
 * Default OutputHandler 
 */
public final class DefaultOutputHandler<V> implements MTSOperator.OutputHandler<V> {
  
  private static final Logger LOG = Logger.getLogger(DefaultOutputHandler.class.getName());
  
  @Inject
  public DefaultOutputHandler() {
    
  }
  
  @Override
  public void onNext(final WindowOutput<V> output) {
    LOG.log(Level.INFO, output.toString());
  }

}
