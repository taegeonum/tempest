package edu.snu.tempest.operators.common;

/**
 * Subscription interface.
 */
public interface Subscription<Token> {

  /**
   * Return a token of the subscription.
   * @return token
   */
  Token getToken();

  /**
   * Unsubscribe the subscription.
   */
  void unsubscribe();
}