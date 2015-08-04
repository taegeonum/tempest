/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package edu.snu.tempest.operator.common;

import java.util.Collection;
import java.util.concurrent.locks.ReadWriteLock;
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
   * Read/write lock.
   */
  private final ReadWriteLock lock;

  /**
   * DefaultSubscription.
   * @param container a container subscribing this subscription.
   * @param val a value
   * @param token a token for identifying the subscription
   */
  public DefaultSubscription(final Collection<T> container,
      final T val,
      final Token token,
      final ReadWriteLock lock) {
    this.val = val;
    this.container = container;
    this.token = token;
    this.lock = lock;
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
    this.lock.writeLock().lock();
    container.remove(val);
    this.lock.writeLock().unlock();
  }
  
}