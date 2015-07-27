package edu.snu.tempest.operators.staticmts.impl;

import edu.snu.tempest.operators.Timescale;
import edu.snu.tempest.operators.staticmts.StaticRelationGraph;
import edu.snu.tempest.utils.TestAggregator;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.*;

public class StaticSlicedWindowOperatorTest {
  StaticRelationGraph<Map<Integer, Integer>> relationGraph;
  List<Timescale> timescales;
  IntegerRef counter;
  TestAggregator aggregator;
  
  @Before
  public void initialize() {
    relationGraph = mock(StaticRelationGraph.class);
    timescales = new LinkedList<>();
    counter = new IntegerRef(0);
    timescales.add(new Timescale(5, 3));
    aggregator = new TestAggregator();
  }

  /**
   * SlicedWindowOperator should aggregate the input.
   */
  @Test
  public void defaultSlicedWindowTest() {
    final StaticSlicedWindowOperatorImpl<Integer, Map<Integer, Integer>> operator =
        new StaticSlicedWindowOperatorImpl<>(aggregator, relationGraph, 0L);

    when(relationGraph.nextSliceTime()).thenReturn(1L, 3L, 4L, 6L, 7L, 9L, 10L, 12L);

    final Map<Integer, Integer> result = new HashMap<>();
    result.put(1, 3); result.put(2, 1); result.put(3, 1);
    operator.execute(1); operator.execute(2); operator.execute(3);
    operator.execute(1); operator.execute(1);
    operator.onNext(1L);
    verify(relationGraph).savePartialOutput(0, 1, result);

    final Map<Integer, Integer> result2 = new HashMap<>();
    result2.put(1, 2); result2.put(4, 1); result2.put(5, 1); result2.put(3, 1);
    operator.execute(4); operator.execute(5); operator.execute(3);
    operator.execute(1); operator.execute(1);

    operator.onNext(3L);
    verify(relationGraph).savePartialOutput(1, 3, result2);

    final Map<Integer, Integer> result3 = new HashMap<>();
    result3.put(1, 2); result3.put(4, 1);
    operator.execute(4); operator.execute(1); operator.execute(1);

    operator.onNext(4L);
    verify(relationGraph).savePartialOutput(3, 4, result3);

    operator.onNext(6L);
    verify(relationGraph).savePartialOutput(4, 6, new HashMap<Integer, Integer>());
  }
  
  class IntegerRef {
    public int value;
    
    public IntegerRef(int i) {
      this.value = i;
    }
  }
}
