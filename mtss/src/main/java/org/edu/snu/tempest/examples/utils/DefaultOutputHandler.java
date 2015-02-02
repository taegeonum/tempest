package org.edu.snu.tempest.examples.utils;

import org.edu.snu.tempest.operator.MTSOperator.OutputHandler;
import org.edu.snu.tempest.operator.WindowOutput;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
 * Default OutputHandler 
 */
public class DefaultOutputHandler<V> implements OutputHandler<V> {
  
  private static final Logger LOG = Logger.getLogger(DefaultOutputHandler.class.getName());
  
  @Inject
  public DefaultOutputHandler() {
    
  }
  
  @Override
  public void onNext(WindowOutput<V> output) {
    LOG.log(Level.INFO, output.toString());
  }

}
