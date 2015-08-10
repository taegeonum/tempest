/*
 * Copyright (C) 2015 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.tempest.operator.common;

import java.util.logging.Logger;

/**
 * Default Subscription.
 */
public final class DefaultSubscription<T, Token> implements Subscription<Token> {
  private static final Logger LOG = Logger.getLogger(DefaultSubscription.class.getName());

  /**
   * A token for identifying the subscription.
   */
  private final Token token;

  /**
   * Subscription handler for unsubscribe.
   */
  private final SubscriptionHandler<Token> handler;

  /**
   * DefaultSubscription.
   * @param handler a handler for unsubscribe this subscription.
   * @param token a token for identifying the subscription
   */
  public DefaultSubscription(final SubscriptionHandler<Token> handler,
      final Token token) {
    this.handler = handler;
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
    handler.unsubscribe(this);
  }
  
}