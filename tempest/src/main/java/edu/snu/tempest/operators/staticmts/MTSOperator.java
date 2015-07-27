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
package edu.snu.tempest.operators.staticmts;

import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Stage;
import edu.snu.tempest.examples.utils.DefaultOutputHandler;
import edu.snu.tempest.operators.common.MTSWindowOutput;

/**
 * Static MTS operator interface.
 * It receives multiple timescales at starting time
 * and produces multi-time scale outputs.
 */
public interface MTSOperator<I> extends Stage {

  /**
   * Start of MTSOperator.
   */
  void start();
  
  /**
   * It receives input from this function.
   * 
   * @param val input value
   */
  void execute(final I val);

  /**
   * MTSOperator sends window outputs to OutputHandler.
   */
  @DefaultImplementation(DefaultOutputHandler.class)
  public interface MTSOutputHandler<V> extends EventHandler<MTSWindowOutput<V>> {

  }
}
