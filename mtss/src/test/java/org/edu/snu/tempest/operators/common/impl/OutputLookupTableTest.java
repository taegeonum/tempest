package org.edu.snu.tempest.operators.common.impl;

import junit.framework.Assert;
import org.edu.snu.tempest.operators.common.NotFoundException;
import org.edu.snu.tempest.operators.common.OutputLookupTable;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

public class OutputLookupTableTest {

  Map<Integer, Integer> output;
  OutputLookupTable<Map<Integer, Integer>> table;

  @Before
  public void initialize() {
    output = new HashMap<>();
    output.put(1, 10);
    
    table = new DefaultOutputLookupTableImpl<>();
    table.saveOutput(0, 10, output);
  }
  
  @Test
  public void lookupTest() throws NotFoundException {
    final Map<Integer, Integer> luo = table.lookup(0, 10);
    Assert.assertEquals(luo, output);
  }
  
  @Test
  public void lookupRowTest() {
    final Map<Integer, Integer> output2 = new HashMap<>();
    output2.put(1, 20);
    
    table.saveOutput(0, 5, output2);

    final Map<Integer, Integer> output3 = new HashMap<>();
    output3.put(1, 20);
    
    table.saveOutput(0, 8, output3);

    final TreeMap<Long, Map<Integer, Integer>> maps = table.lookup(0);

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
  
  @Test
  public void lookupLargestOutputTest() throws NotFoundException {
    final Map<Integer, Integer> output2 = new HashMap<>();
    output2.put(1, 20);
    
    table.saveOutput(0, 5, output2);

    final Map<Integer, Integer> output3 = new HashMap<>();
    output3.put(1, 20);
    
    table.saveOutput(0, 8, output3);
    
    Assert.assertEquals(output3, table.lookupLargestSizeOutput(0, 9).value);
    Assert.assertEquals(output, table.lookupLargestSizeOutput(0, 11).value);
    Assert.assertEquals(output2, table.lookupLargestSizeOutput(0, 7).value);
    Assert.assertEquals(output3, table.lookupLargestSizeOutput(0, 8).value);
  }
  
  @Test
  public void deleteRowTest() {
    final Map<Integer, Integer> output2 = new HashMap<>();
    output2.put(1, 20);
    
    table.saveOutput(0, 5, output2);

    final Map<Integer, Integer> output3 = new HashMap<>();
    output3.put(1, 20);
    
    table.saveOutput(0, 8, output3);
  
    table.deleteRow(0);
    Assert.assertEquals(table.lookup(0), null);
  }
}
