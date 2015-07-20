package org.edu.snu.tempest.operator;


/**
 * Subscription interface.
 */
public interface Subscription<Token> {

  Token getToken();
  void unsubscribe();
}