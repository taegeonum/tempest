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
package edu.snu.tempest.operators.dynamicmts.impl;

import edu.snu.tempest.operators.Timescale;
import edu.snu.tempest.operators.common.OutputLookupTable;
import edu.snu.tempest.operators.dynamicmts.DynamicRelationCube;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

import static org.mockito.Mockito.*;

public final class GarbageCollectorTest {

  /**
   * default garbage collector deletes outputs according to the largest window size.
   */
  @Test
  public void onTimescaleAdditionTest() {
    final List<Timescale> timescales = new LinkedList<>();
    timescales.add(new Timescale(3,1));

    final OutputLookupTable<?> table = mock(OutputLookupTable.class);
    final DynamicRelationCube.GarbageCollector gc = new DefaultGarbageCollectorImpl(timescales, table, 0);

    gc.onNext(3L);
    verify(table, never()).deleteOutputs(0);
    gc.onNext(4L);
    verify(table, times(1)).deleteOutputs(0);
    gc.onNext(5L);
    verify(table, times(1)).deleteOutputs(1);
    
    gc.onTimescaleAddition(new Timescale(5,1), 5L);
    gc.onNext(6L);
    verify(table, never()).deleteOutputs(2);
    
    gc.onNext(7L);
    verify(table, never()).deleteOutputs(2);
    
    gc.onNext(8L);
    verify(table, times(1)).deleteOutputs(2);

    gc.onNext(9L);
    verify(table, times(1)).deleteOutputs(3);
  }

  /**
   * default garbage collector deletes outputs according to the largest window size.
   */
  @Test
  public void onTimescaleDeletionTest() {
    final List<Timescale> timescales = new LinkedList<>();
    final Timescale t1 = new Timescale(3,1);
    final Timescale t2 = new Timescale(10,2);
    
    timescales.add(t1);
    timescales.add(t2);

    final OutputLookupTable<?> table = mock(OutputLookupTable.class);
    final DynamicRelationCube.GarbageCollector gc = new DefaultGarbageCollectorImpl(timescales, table, 0);

    gc.onNext(10L);
    verify(table, never()).deleteOutputs(0);
    gc.onNext(11L);
    verify(table, times(1)).deleteOutputs(0);
    gc.onNext(12L);
    verify(table, times(1)).deleteOutputs(1);

    // delete timescale
    gc.onTimescaleDeletion(t2);
    gc.onNext(13L);
    verify(table, times(1)).deleteOutputs(2);
    verify(table, times(1)).deleteOutputs(3);
    verify(table, times(1)).deleteOutputs(4);
    verify(table, times(1)).deleteOutputs(5);
    verify(table, times(1)).deleteOutputs(6);
    verify(table, times(1)).deleteOutputs(7);
    verify(table, times(1)).deleteOutputs(8);
    verify(table, times(1)).deleteOutputs(9);
    verify(table, never()).deleteOutputs(10);

    gc.onNext(14L);
    verify(table, times(1)).deleteOutputs(10);
  }
}
