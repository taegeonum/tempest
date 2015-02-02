package org.edu.snu.tempest.operator;


/*
 * Subscription interface
 */
public interface Subscription<Token> {

  public Token getToken();
  public void unsubscribe();
}