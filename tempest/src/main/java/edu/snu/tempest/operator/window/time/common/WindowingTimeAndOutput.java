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
package edu.snu.tempest.operator.window.time.common;

/**
 * This contains windowing time and output.
 * @param <V> val
 */
public final class WindowingTimeAndOutput<V> {
  public final long startTime;
  public final long endTime;
  public final V output;

  /**
   * This contains windowing time and output.
   * @param startTime a start time of the output
   * @param endTime a end time of the output
   * @param output an output
   */
  public WindowingTimeAndOutput(final long startTime, final long endTime, final V output) {
    this.startTime = startTime;
    this.endTime = endTime;
    this.output = output;
  }
  
  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("[");
    sb.append(startTime);
    sb.append("-");
    sb.append(endTime);
    sb.append("] ");
    sb.append(this.output);
    return sb.toString();
  }
}
