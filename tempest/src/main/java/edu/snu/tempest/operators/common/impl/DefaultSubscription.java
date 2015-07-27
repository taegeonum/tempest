package edu.snu.tempest.operators.common.impl;

import edu.snu.tempest.operators.common.Subscription;

import javax.inject.Inject;
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Default Subscription.
 */
public final class DefaultSubscription<T, Token> implements Subscription<Token> {

  private static final Logger LOG = Logger.getLogger(DefaultSubscription.class.getName());

  /**
   * A value for the subscription.
   */
  private final T val;

  /**
   * A token for identifying the subscription.
   */
  private final Token token;

  /**
   * Container subscribing this subscription.
   */
  private final Collection<T> container;

  /**
   * DefaultSubscription.
   * @param container a container subscribing this subscription.
   * @param val a value
   * @param token a token for identifying the subscription
   */
  @Inject
  public DefaultSubscription(final Collection<T> container, 
      final T val,
      final Token token) {
    this.val = val;
    this.container = container;
    this.token = token;
  }

  /**
   * Return the token.
   * @return token
   */
  @Override
  public Token getToken() {
    return token;
  }

  /**
   * Unsubscribe the subscription from the container.
   */
  @Override
  public void unsubscribe() {
    LOG.log(Level.FINE, "Unsubscribe " + token);
    synchronized (container) {
      container.remove(val);
    }
  }
  
}