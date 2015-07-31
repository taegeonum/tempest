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
package edu.snu.tempest.operator.window.common;

import edu.snu.tempest.operator.window.Timescale;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

import static org.mockito.Mockito.*;

public final class DynamicOutputCleanerTest {

  /**
   * dynamic output cleaner deletes outputs of table according to the largest window size.
   */
  @Test
  public void onTimescaleAdditionTest() {
    final List<Timescale> timescales = new LinkedList<>();
    timescales.add(new Timescale(3,1));

    final OutputLookupTable<?> table = mock(OutputLookupTable.class);
    final DynamicOutputCleaner outputCleaner = new DynamicOutputCleaner(timescales, table, 0);

    outputCleaner.onNext(3L);
    verify(table, never()).deleteOutputs(0);
    outputCleaner.onNext(4L);
    verify(table, times(1)).deleteOutputs(0);
    outputCleaner.onNext(5L);
    verify(table, times(1)).deleteOutputs(1);

    outputCleaner.addTimescale(new Timescale(5, 1), 5L);
    outputCleaner.onNext(6L);
    verify(table, never()).deleteOutputs(2);

    outputCleaner.onNext(7L);
    verify(table, never()).deleteOutputs(2);

    outputCleaner.onNext(8L);
    verify(table, times(1)).deleteOutputs(2);

    outputCleaner.onNext(9L);
    verify(table, times(1)).deleteOutputs(3);
  }

  /**
   * dynamic output cleaner deletes outputs of table according to the largest window size.
   */
  @Test
  public void onTimescaleDeletionTest() {
    final List<Timescale> timescales = new LinkedList<>();
    final Timescale t1 = new Timescale(3,1);
    final Timescale t2 = new Timescale(10,2);
    
    timescales.add(t1);
    timescales.add(t2);

    final OutputLookupTable<?> table = mock(OutputLookupTable.class);
    final DynamicOutputCleaner outputCleaner = new DynamicOutputCleaner(timescales, table, 0);

    outputCleaner.onNext(10L);
    verify(table, never()).deleteOutputs(0);
    outputCleaner.onNext(11L);
    verify(table, times(1)).deleteOutputs(0);
    outputCleaner.onNext(12L);
    verify(table, times(1)).deleteOutputs(1);

    // delete timescale
    outputCleaner.removeTimescale(t2);
    outputCleaner.onNext(13L);
    verify(table, times(1)).deleteOutputs(2);
    verify(table, times(1)).deleteOutputs(3);
    verify(table, times(1)).deleteOutputs(4);
    verify(table, times(1)).deleteOutputs(5);
    verify(table, times(1)).deleteOutputs(6);
    verify(table, times(1)).deleteOutputs(7);
    verify(table, times(1)).deleteOutputs(8);
    verify(table, times(1)).deleteOutputs(9);
    verify(table, never()).deleteOutputs(10);

    outputCleaner.onNext(14L);
    verify(table, times(1)).deleteOutputs(10);
  }
}
