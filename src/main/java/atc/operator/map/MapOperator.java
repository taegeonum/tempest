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
package atc.operator.map;


import edu.snu.tempest.operator.Operator;
import edu.snu.tempest.operator.OutputEmitter;

import javax.inject.Inject;

/**
 * Map operator which maps input.
 * @param <I> input
 */
public final class MapOperator<I, O> implements Operator<I, O> {

  /**
   * Map function.
   */
  private final MapFunction<I, O> mapFunc;

  /**
   * Next operator.
   */
  private OutputEmitter<O> outputEmitter;

  /**
   * Map operator.
   * @param mapFunc a map function
   */
  @Inject
  private MapOperator(final MapFunction<I, O> mapFunc) {
    this.mapFunc = mapFunc;
  }

  @Override
  public void execute(final I val) {
    outputEmitter.emit(mapFunc.map(val));
  }

  @Override
  public void prepare(final OutputEmitter<O> emitter) {
    this.outputEmitter = emitter;
  }
}