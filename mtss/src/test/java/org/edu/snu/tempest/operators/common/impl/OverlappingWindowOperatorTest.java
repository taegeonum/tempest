package org.edu.snu.tempest.operators.common.impl;

import org.edu.snu.tempest.operators.Timescale;
import org.edu.snu.tempest.operators.common.NotFoundException;
import org.edu.snu.tempest.operators.common.OverlappingWindowOperator;
import org.edu.snu.tempest.operators.staticmts.MTSOperator;
import org.edu.snu.tempest.operators.staticmts.StaticRelationGraph;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class OverlappingWindowOperatorTest {

  @Test
  public void overlappingWindowOperatorTest() throws NotFoundException {

    Map<Integer, Integer> map = new HashMap<>();
    map.put(1, 1);
    Timescale ts = new Timescale(5, 3);
    StaticRelationGraph<Map<Integer, Integer>> cube = mock(StaticRelationGraph.class);
    MTSOperator.OutputHandler<Map<Integer, Integer>> outputHandler = mock(MTSOperator.OutputHandler.class);
    OverlappingWindowOperator<Map<Integer, Integer>> operator = new DefaultOverlappingWindowOperatorImpl<>(
        ts, cube, outputHandler, 0L);
    operator.onNext(3L);
    verify(cube).finalAggregate(-2, 3, ts);
    operator.onNext(6L);
    verify(cube).finalAggregate(1, 6, ts);
  }
}
