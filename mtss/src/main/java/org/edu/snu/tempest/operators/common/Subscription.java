package org.edu.snu.tempest.operators.common;

/**
 * Subscription interface.
 */
public interface Subscription<Token> {

  Token getToken();
  void unsubscribe();
}