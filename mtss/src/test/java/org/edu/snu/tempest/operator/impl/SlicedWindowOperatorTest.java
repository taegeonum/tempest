package org.edu.snu.tempest.operator.impl;

import org.edu.snu.tempest.Timescale;
import org.edu.snu.tempest.operator.relationcube.RelationCube;
import org.edu.snu.tempest.utils.TestAggregator;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.*;

public class SlicedWindowOperatorTest {
  RelationCube<Map<Integer, Integer>> cube;
  List<Timescale> timescales;
  IntegerRef counter;
  TestAggregator aggregator;
  
  @Before
  public void initialize() {
    cube = mock(RelationCube.class);
    timescales = new LinkedList<>();
    counter = new IntegerRef(0);
    timescales.add(new Timescale(5, 3));
    aggregator = new TestAggregator();
  }

  /*
   * SlicedWindowOperator should aggregate the input 
   */
  @Test
  public void defaultSlicedWindowTest() {
    DefaultSlicedWindowOperatorImpl<Integer, Map<Integer, Integer>> operator =
        new DefaultSlicedWindowOperatorImpl<>(aggregator, timescales, cube);

    Map<Integer, Integer> result = new HashMap<>();
    result.put(1, 3); result.put(2, 1); result.put(3, 1);
    operator.execute(1); operator.execute(2); operator.execute(3);
    operator.execute(1); operator.execute(1);
    operator.onNext(new LogicalTime(1));
    verify(cube).savePartialOutput(0, 1, result);

    Map<Integer, Integer> result2 = new HashMap<>();
    result2.put(1, 2); result2.put(4, 1); result2.put(5, 1); result2.put(3, 1);
    operator.execute(4); operator.execute(5); operator.execute(3);
    operator.execute(1); operator.execute(1);

    operator.onNext(new LogicalTime(3));
    verify(cube).savePartialOutput(1, 3, result2);

    Map<Integer, Integer> result3 = new HashMap<>();
    result3.put(1, 2); result3.put(4, 1);
    operator.execute(4); operator.execute(1); operator.execute(1);

    operator.onNext(new LogicalTime(4));
    verify(cube).savePartialOutput(3, 4, result3);

    operator.onNext(new LogicalTime(6));
    verify(cube).savePartialOutput(4, 6, new HashMap<Integer, Integer>());
  }
  
  class IntegerRef {
    public int value;
    
    public IntegerRef(int i) {
      this.value = i;
    }
  }
}
