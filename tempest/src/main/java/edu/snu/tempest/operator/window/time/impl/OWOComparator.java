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
package edu.snu.tempest.operator.window.time.impl;

import java.util.Comparator;

/**
 * An overlapping window operator which has small size of window
 * is executed before another OWOs having large size of window.
 */
final class OWOComparator implements Comparator<OverlappingWindowOperator> {

  @Override
  public int compare(OverlappingWindowOperator x, OverlappingWindowOperator y) {
    if (x.getTimescale().windowSize < y.getTimescale().windowSize) {
      return -1;
    } else if (x.getTimescale().windowSize > y.getTimescale().windowSize) {
      return 1;
    } else {
      return 0;
    }
  }
}