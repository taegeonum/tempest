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

/**
 * An output emitter connecting previous operator to next operator.
 * It sends emitted data to next operator.
 */
public final class NextOperatorConnectionEmitter<I, O> implements OutputEmitter<I> {

  /**
   * Next operator which receives emitted data.
   */
  private final Operator<I, O> nextOperator;

  /**
   * An output emitter connecting previous to next operator.
   * It emits received input to next operator.
   * @param nextOperator
   */
  public NextOperatorConnectionEmitter(final Operator<I, O> nextOperator) {
    this.nextOperator = nextOperator;
  }

  @Override
  public void emit(final I input) {
    nextOperator.execute(input);
  }
}
