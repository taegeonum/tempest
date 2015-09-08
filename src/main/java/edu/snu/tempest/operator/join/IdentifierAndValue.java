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
package edu.snu.tempest.operator.join;

/**
 * This class contains identifier and value of JoinInput.
 * Identifier shows the type of input.
 * Value is real data of JoinInput.
 */
public final class IdentifierAndValue<V> {

  /**
   * An identifier of JoinInput.
   */
  public final String identifier;

  /**
   * Value of JoinInput.
   */
  public final V value;

  /**
   * A target for join.
   * @param identifier an identifier
   * @param value a value
   */
  public IdentifierAndValue(final String identifier,
                            final V value) {
    this.identifier = identifier;
    this.value = value;
  }
}
