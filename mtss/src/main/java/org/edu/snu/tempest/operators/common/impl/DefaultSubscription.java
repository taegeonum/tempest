package org.edu.snu.tempest.operators.common.impl;

import org.edu.snu.tempest.operators.common.Subscription;

import javax.inject.Inject;
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Subscription.
 */
public final class DefaultSubscription<T, Token> implements Subscription<Token> {

  private static final Logger LOG = Logger.getLogger(DefaultSubscription.class.getName());
  
  private final T val;
  private final Token token;
  private final Collection<T> container;
  
  @Inject
  public DefaultSubscription(final Collection<T> container, 
      final T val,
      final Token token) {
    this.val = val;
    this.container = container;
    this.token = token;
  }
  
  @Override
  public Token getToken() {
    return token;
  }

  @Override
  public void unsubscribe() {
    LOG.log(Level.FINE, "Unsubscribe " + token);
    synchronized (container) {
      container.remove(val);
    }
  }
  
}