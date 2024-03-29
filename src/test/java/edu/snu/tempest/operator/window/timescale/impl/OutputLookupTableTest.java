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

import edu.snu.tempest.operator.common.NotFoundException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class OutputLookupTableTest {

  Map<Integer, Integer> output;
  DefaultOutputLookupTableImpl<Map<Integer, Integer>> table;

  @Before
  public void initialize() {
    output = new HashMap<>();
    output.put(1, 10);
    table = new DefaultOutputLookupTableImpl<>();
    table.saveOutput(0, 10, output);
  }

  /**
   * Save outputs in multiple threads and check whether the outputs are saved well.
   * Multiple threads save outputs into an output lookup table.
   * After that, checks whether the saved outputs are same as what the multiple threads saved.
   */
  @Test
  public void saveOutputWithMultipleThreads() throws NotFoundException, InterruptedException {
    final DefaultOutputLookupTableImpl<Map<Integer, Integer>> lookupTable = new DefaultOutputLookupTableImpl<>();
    final ExecutorService executor = Executors.newFixedThreadPool(10);
    for (int startTime = 0; startTime < 100; startTime++) {
      final int start = startTime;
      for (int endTime = startTime + 1; endTime < startTime + 100; endTime++) {
        final int end = endTime;
        executor.submit(new Runnable() {
          @Override
          public void run() {
            final Map<Integer, Integer> o = new HashMap<Integer, Integer>();
            o.put(start, end);
            lookupTable.saveOutput(start, end, o);
          }
        });
      }
    }

    executor.awaitTermination(10, TimeUnit.SECONDS);
    executor.shutdown();
    for (int startTime = 0; startTime < 100; startTime++) {
      for (int endTime = startTime + 1; endTime < startTime + 100; endTime++) {
        final Map<Integer, Integer> o = lookupTable.lookup(startTime, endTime);
        final Map<Integer, Integer> expected = new HashMap<>();
        expected.put(startTime, endTime);
        Assert.assertEquals(expected, o);
      }
    }
  }

  /**
   * Output lookup table lookup test
   */
  @Test
  public void lookupTest() throws NotFoundException {
    final Map<Integer, Integer> luo = table.lookup(0, 10);
    Assert.assertEquals(luo, output);
  }

  /**
   * Test for lookup multiple outputs which start at 0.
   */
  @Test
  public void lookupRowTest() throws NotFoundException {
    final Map<Integer, Integer> output2 = new HashMap<>();
    output2.put(1, 20);
    
    table.saveOutput(0, 5, output2);

    final Map<Integer, Integer> output3 = new HashMap<>();
    output3.put(1, 20);
    
    table.saveOutput(0, 8, output3);

    final ConcurrentSkipListMap<Long, Map<Integer, Integer>> maps = table.lookup(0);

    final Entry<Long, Map<Integer, Integer>> first = maps.pollFirstEntry();
    final Entry<Long, Map<Integer, Integer>> second = maps.pollFirstEntry();
    final Entry<Long, Map<Integer, Integer>> third = maps.pollFirstEntry();

    Assert.assertEquals(5L, (long)first.getKey());
    Assert.assertEquals(output2, first.getValue());

    Assert.assertEquals(8L, (long)second.getKey());
    Assert.assertEquals(output3, second.getValue());
    
    Assert.assertEquals(10L, (long)third.getKey());
    Assert.assertEquals(output, third.getValue());
  }

  /**
   * Test for looking up an output having largest endTime within outputs which start at startTime.
   */
  @Test
  public void lookupLargestOutputTest() throws NotFoundException {
    final Map<Integer, Integer> output2 = new HashMap<>();
    output2.put(1, 20);
    
    table.saveOutput(0, 5, output2);

    final Map<Integer, Integer> output3 = new HashMap<>();
    output3.put(1, 20);
    
    table.saveOutput(0, 8, output3);
    
    Assert.assertEquals(output3, table.lookupLargestSizeOutput(0, 9).output);
    Assert.assertEquals(output, table.lookupLargestSizeOutput(0, 11).output);
    Assert.assertEquals(output2, table.lookupLargestSizeOutput(0, 7).output);
    Assert.assertEquals(output3, table.lookupLargestSizeOutput(0, 8).output);
  }

  /**
   * Test for deleting outputs which start at 0
   */
  @Test
  public void deleteOutputsTest() throws NotFoundException {
    final Map<Integer, Integer> output2 = new HashMap<>();
    output2.put(1, 20);
    
    table.saveOutput(0, 5, output2);

    final Map<Integer, Integer> output3 = new HashMap<>();
    output3.put(1, 20);
    
    table.saveOutput(0, 8, output3);
  
    table.deleteOutputs(0);
    Assert.assertEquals(table.lookup(0), null);
  }
}
