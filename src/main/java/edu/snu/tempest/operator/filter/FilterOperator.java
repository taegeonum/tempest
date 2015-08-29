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
package edu.snu.tempest.operator.filter;

import edu.snu.tempest.operator.Operator;
import edu.snu.tempest.operator.OutputEmitter;

import javax.inject.Inject;

/**
 * Filter operator which filters input.
 * @param <I> input
 */
public final class FilterOperator<I> implements Operator<I, I> {

  /**
   * Filter function.
   */
  private final FilterFunc<I> filterFunc;

  /**
   * Next operator.
   */
  private OutputEmitter<I> outputEmitter;

  /**
   * Filter operator.
   * @param filterFunc a filter function
   */
  @Inject
  private FilterOperator(final FilterFunc<I> filterFunc) {
    this.filterFunc = filterFunc;
  }

  /**
   * Filters the input value.
   * @param val input value
   */
  @Override
  public void execute(final I val) {
    if (!filterFunc.filter(val)) {
      outputEmitter.emit(val);
    }
  }

  @Override
  public void prepare(final OutputEmitter<I> emitter) {
    this.outputEmitter = emitter;
  }

}