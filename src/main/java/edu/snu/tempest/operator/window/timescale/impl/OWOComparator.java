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
package edu.snu.tempest.operator.window.timescale.impl;

import java.util.Comparator;

/**
 * An overlapping window operator which has small sized window
 * is executed before another OWO with a large sized window.
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