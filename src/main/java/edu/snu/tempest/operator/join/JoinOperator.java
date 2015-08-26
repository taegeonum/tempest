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

import edu.snu.tempest.operator.Operator;
import edu.snu.tempest.operator.OutputEmitter;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Join operator which joins inputs.
 */
public final class JoinOperator<K, O> implements Operator<JoinInput<K>, O> {

  /**
   * Join function.
   */
  private final JoinFunc<K, O> joinFunc;

  /**
   * Join condition.
   */
  private final JoinCondition<K> joinCondition;

  /**
   * Next operator.
   */
  private OutputEmitter<O> outputEmitter;

  /**
   * A map for containing join inputs.
   */
  private final ConcurrentMap<K, List<IdentifierAndValue>> joinInputMap;

  /**
   * Join operator.
   * @param joinFunc a join function
   */
  @Inject
  private JoinOperator(final JoinFunc<K, O> joinFunc,
                       final JoinCondition<K> joinCondition) {
    this.joinFunc = joinFunc;
    this.joinCondition = joinCondition;
    this.joinInputMap = new ConcurrentHashMap<>();
  }

  /**
   * Join inputs.
   * @param val join input
   */
  @Override
  public void execute(final JoinInput<K> val) {
    final K key = val.getJoinKey();
    List<IdentifierAndValue> inputs = joinInputMap.get(key);
    if (inputs == null) {
      joinInputMap.putIfAbsent(key, new LinkedList<IdentifierAndValue>());
      inputs = joinInputMap.get(key);
    }

    synchronized (inputs) {
      final IdentifierAndValue idAndVal = new IdentifierAndValue(val.getIdentifier(), val.getValue());
      inputs.add(idAndVal);
      if (joinCondition.readyToJoin(key, idAndVal)) {
        // join the aggregated inputs and flush
        outputEmitter.emit(joinFunc.join(key, inputs));
        joinInputMap.remove(key);
        joinCondition.reset(key);
      }
    }
  }

  @Override
  public void prepare(final OutputEmitter<O> emitter) {
    this.outputEmitter = emitter;
  }
}