package org.edu.snu.tempest.operator.impl;

import org.edu.snu.tempest.Timescale;
import org.edu.snu.tempest.operator.MTSOperator;
import org.edu.snu.tempest.operator.OverlappingWindowOperator;
import org.edu.snu.tempest.operator.relationcube.RelationCube;
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
    RelationCube<Map<Integer, Integer>> cube = mock(RelationCube.class);
    MTSOperator.OutputHandler<Map<Integer, Integer>> outputHandler = mock(MTSOperator.OutputHandler.class);
    OverlappingWindowOperator<Map<Integer, Integer>> operator = new DefaultOverlappingWindowOperatorImpl<>(
        ts, cube, outputHandler, 0L);
    operator.onNext(3L);
    verify(cube).finalAggregate(-2, 3, ts);
    operator.onNext(6L);
    verify(cube).finalAggregate(1, 6, ts);
  }
}
