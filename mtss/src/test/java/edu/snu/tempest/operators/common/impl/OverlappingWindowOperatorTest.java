package edu.snu.tempest.operators.common.impl;

import edu.snu.tempest.operators.Timescale;
import edu.snu.tempest.operators.common.NotFoundException;
import edu.snu.tempest.operators.common.OverlappingWindowOperator;
import edu.snu.tempest.operators.staticmts.MTSOperator;
import edu.snu.tempest.operators.staticmts.StaticRelationGraph;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public final class OverlappingWindowOperatorTest {

  /**
   * Overlapping window operator should call relationCube.finalAggregate
   * every its interval.
   */
  @Test
  public void overlappingWindowOperatorTest() throws NotFoundException {
    final Map<Integer, Integer> map = new HashMap<>();
    map.put(1, 1);
    final Timescale ts = new Timescale(5, 3);
    final StaticRelationGraph<Map<Integer, Integer>> cube = mock(StaticRelationGraph.class);
    final MTSOperator.MTSOutputHandler<Map<Integer, Integer>> outputHandler = mock(MTSOperator.MTSOutputHandler.class);
    final OverlappingWindowOperator<Map<Integer, Integer>> operator = new DefaultOverlappingWindowOperatorImpl<>(
        ts, cube, outputHandler, 0L);
    operator.onNext(3L);
    verify(cube).finalAggregate(-2, 3, ts);
    operator.onNext(6L);
    verify(cube).finalAggregate(1, 6, ts);
  }
}
