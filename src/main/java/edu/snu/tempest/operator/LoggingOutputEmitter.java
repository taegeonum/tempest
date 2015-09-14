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
package edu.snu.tempest.operator;

import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * An output emitter logging emitted outputs.
 */
public final class LoggingOutputEmitter<V> implements OutputEmitter<V> {
  private static final Logger LOG = Logger.getLogger(LoggingOutputEmitter.class.getName());

  /**
   * An output emitter logging emitted outputs.
   */
  public LoggingOutputEmitter() {
  }

  @Override
  public void emit(final V input) {
    LOG.log(Level.INFO, input.toString());
  }
}
