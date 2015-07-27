package edu.snu.tempest.operators.dynamicmts.impl;

import edu.snu.tempest.operators.Timescale;
import edu.snu.tempest.operators.common.Aggregator;
import edu.snu.tempest.operators.common.aggregators.CountByKeyAggregator;
import edu.snu.tempest.operators.dynamicmts.DynamicRelationCube;
import edu.snu.tempest.utils.MTSTestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class DynamicRelationCubeTest {

  /**
   * test relationCube final aggregation.
   */
  @Test
  public void relationCubeAggregationTest() {
    final List<Timescale> timescales = new LinkedList<>();
    final Timescale ts1 = new Timescale(4, 2);
    final Timescale ts2 = new Timescale(6, 3);
    final Timescale ts3 = new Timescale(10, 4);
    timescales.add(ts1); timescales.add(ts2); timescales.add(ts3);
    final long startTime = 0;
    final Aggregator<Integer, Map<Integer, Long>> aggregator =
        new CountByKeyAggregator<Integer, Integer>(new CountByKeyAggregator.KeyExtractor<Integer, Integer>() {
          @Override
          public Integer getKey(final Integer value) {
            return value;
          }
        });

    final DynamicRelationCube<Map<Integer, Long>> relationCube =
        new DynamicRelationCubeImpl<>(timescales, aggregator, new CachingRatePolicy(timescales, 1), startTime);

    final Map<Integer, Long> partialOutput1 = new HashMap<>();
    partialOutput1.put(1, 10L); partialOutput1.put(2, 15L);

    final Map<Integer, Long> partialOutput2 = new HashMap<>();
    partialOutput2.put(3, 5L); partialOutput2.put(2, 15L);

    final Map<Integer, Long> partialOutput3 = new HashMap<>();
    partialOutput3.put(4, 10L); partialOutput3.put(3, 15L);

    final Map<Integer, Long> partialOutput4 = new HashMap<>();
    partialOutput4.put(5, 10L);

    final Map<Integer, Long> partialOutput5 = new HashMap<>();
    partialOutput5.put(4, 10L);

    final Map<Integer, Long> partialOutput6 = new HashMap<>();
    partialOutput6.put(2, 10L);

    final Map<Integer, Long> partialOutput7 = new HashMap<>();
    partialOutput7.put(3, 10L);

    final Map<Integer, Long> partialOutput8 = new HashMap<>();
    partialOutput8.put(5, 10L);

    final Map<Integer, Long> partialOutput9 = new HashMap<>();
    partialOutput9.put(3, 10L);

    final Map<Integer, Long> partialOutput10 = new HashMap<>();
    partialOutput10.put(2, 10L);

    final Map<Integer, Long> partialOutput11 = new HashMap<>();
    partialOutput11.put(3, 10L);

    relationCube.savePartialOutput(0, 2, partialOutput1);
    // [w=4, i=2] final aggregation at time 2
    final Map<Integer, Long> result1 = relationCube.finalAggregate(-2, 2, ts1);
    Assert.assertEquals(partialOutput1, result1);

    relationCube.savePartialOutput(2, 3, partialOutput2);
    // [w=6, i=3] final aggregation at time 3
    final Map<Integer, Long> result2 = relationCube.finalAggregate(-3, 3, ts2);
    Assert.assertEquals(MTSTestUtils.merge(partialOutput1, partialOutput2), result2);

    relationCube.savePartialOutput(3, 4, partialOutput3);
    // [w=4, i=2] final aggregation at time 4
    final Map<Integer, Long> result3 = relationCube.finalAggregate(0, 4, ts1);
    Assert.assertEquals(MTSTestUtils.merge(partialOutput1, partialOutput2, partialOutput3), result3);
    // [w=10, i=4] final aggregation at time 4
    final Map<Integer, Long> result4 = relationCube.finalAggregate(-6, 4, ts3);
    Assert.assertEquals(MTSTestUtils.merge(partialOutput1, partialOutput2, partialOutput3), result4);

    relationCube.savePartialOutput(4, 6, partialOutput4);
    // [w=4, i=2] final aggregation at time 6
    final Map<Integer, Long> result5 = relationCube.finalAggregate(2, 6, ts1);
    Assert.assertEquals(MTSTestUtils.merge(partialOutput2, partialOutput3, partialOutput4), result5);
    // [w=6, i=3] final aggregation at time 6
    final Map<Integer, Long> result6 = relationCube.finalAggregate(0, 6, ts2);
    Assert.assertEquals(MTSTestUtils.merge(partialOutput1,
        partialOutput2, partialOutput3, partialOutput4), result6);

    relationCube.savePartialOutput(6, 8, partialOutput5);
    // [w=4, i=2] final aggregation at time 8
    final Map<Integer, Long> result7 = relationCube.finalAggregate(4, 8, ts1);
    Assert.assertEquals(MTSTestUtils.merge(partialOutput4, partialOutput5), result7);
    // [w=10, i=4] final aggregation at time 8
    final Map<Integer, Long> result8 = relationCube.finalAggregate(-2, 8, ts3);
    Assert.assertEquals(MTSTestUtils.merge(partialOutput1,
        partialOutput2, partialOutput3, partialOutput4, partialOutput5), result8);

    relationCube.savePartialOutput(8, 9, partialOutput6);
    // [w=6, i=3] final aggregation at time 9
    final Map<Integer, Long> result9 = relationCube.finalAggregate(3, 9, ts2);
    Assert.assertEquals(MTSTestUtils.merge(partialOutput3, partialOutput4,
        partialOutput5, partialOutput6), result9);

    relationCube.savePartialOutput(9, 10, partialOutput7);
    // [w=4, i=2] final aggregation at time 10
    final Map<Integer, Long> result10 = relationCube.finalAggregate(6, 10, ts1);
    Assert.assertEquals(MTSTestUtils.merge(partialOutput5, partialOutput6, partialOutput7), result10);

    relationCube.savePartialOutput(10, 12, partialOutput8);
    // [w=4, i=2] final aggregation at time 12
    final Map<Integer, Long> result11 = relationCube.finalAggregate(8, 12, ts1);
    Assert.assertEquals(MTSTestUtils.merge(partialOutput6, partialOutput7, partialOutput8), result11);
    // [w=6, i=3] final aggregation at time 12
    final Map<Integer, Long> result12 = relationCube.finalAggregate(6, 12, ts2);
    Assert.assertEquals(MTSTestUtils.merge(partialOutput5,
        partialOutput6, partialOutput7, partialOutput8), result12);
    // [w=10, i=4] final aggregation at time 12
    final Map<Integer, Long> result13 = relationCube.finalAggregate(2, 12, ts3);
    Assert.assertEquals(MTSTestUtils.merge(partialOutput2,
        partialOutput3, partialOutput4, partialOutput5,
        partialOutput6, partialOutput7, partialOutput8), result13);

    relationCube.savePartialOutput(12, 14, partialOutput9);
    // [w=4, i=2] final aggregation at time 14
    final Map<Integer, Long> result14 = relationCube.finalAggregate(10, 14, ts1);
    Assert.assertEquals(MTSTestUtils.merge(partialOutput8, partialOutput9), result14);

    relationCube.savePartialOutput(14, 15, partialOutput10);
    // [w=6, i=3] final aggregation at time 15
    final Map<Integer, Long> result15 = relationCube.finalAggregate(9, 15, ts2);
    Assert.assertEquals(MTSTestUtils.merge(partialOutput7, partialOutput8,
        partialOutput9, partialOutput10), result15);

    relationCube.savePartialOutput(15, 16, partialOutput11);
    // [w=4, i=2] final aggregation at time 16
    final Map<Integer, Long> result16 = relationCube.finalAggregate(12, 16, ts1);
    Assert.assertEquals(MTSTestUtils.merge(partialOutput9, partialOutput10, partialOutput11), result16);
    // [w=10, i=4] final aggregation at time 16
    final Map<Integer, Long> result17 = relationCube.finalAggregate(6, 16, ts3);
    Assert.assertEquals(MTSTestUtils.merge(partialOutput5, partialOutput6, partialOutput7,
        partialOutput8, partialOutput9, partialOutput10, partialOutput11), result17);
  }
}
