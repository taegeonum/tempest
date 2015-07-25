package org.edu.snu.tempest.operators.staticmts.impl;

import org.edu.snu.tempest.operators.Timescale;
import org.edu.snu.tempest.operators.common.Aggregator;
import org.edu.snu.tempest.operators.common.aggregators.CountByKeyAggregator;
import org.edu.snu.tempest.operators.staticmts.StaticRelationGraph;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class StaticRelationGraphTest {

  @Test
  public void relationGraphNextSliceTimeTest() {
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

    final StaticRelationGraph<Map<Integer, Long>> relationGraph =
        new StaticRelationGraphImpl<>(timescales, aggregator, startTime);

    Assert.assertEquals(2L, relationGraph.nextSliceTime());
    Assert.assertEquals(3L, relationGraph.nextSliceTime());
    Assert.assertEquals(4L, relationGraph.nextSliceTime());
    Assert.assertEquals(6L, relationGraph.nextSliceTime());
    Assert.assertEquals(8L, relationGraph.nextSliceTime());
    Assert.assertEquals(9L, relationGraph.nextSliceTime());
    Assert.assertEquals(10L, relationGraph.nextSliceTime());
    Assert.assertEquals(12L, relationGraph.nextSliceTime());
    Assert.assertEquals(14L, relationGraph.nextSliceTime());
    Assert.assertEquals(15L, relationGraph.nextSliceTime());
    Assert.assertEquals(16L, relationGraph.nextSliceTime());
    Assert.assertEquals(18L, relationGraph.nextSliceTime());
  }

  @Test
  public void relationGraphAggregationTest() {
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

    final StaticRelationGraph<Map<Integer, Long>> relationGraph =
        new StaticRelationGraphImpl<>(timescales, aggregator, startTime);

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

    relationGraph.savePartialOutput(0, 2, partialOutput1);
    // [w=4, i=2] final aggregation at time 2
    final Map<Integer, Long> result1 = relationGraph.finalAggregate(-2, 2, ts1);
    Assert.assertEquals(partialOutput1, result1);

    relationGraph.savePartialOutput(2, 3, partialOutput2);
    // [w=6, i=3] final aggregation at time 3
    final Map<Integer, Long> result2 = relationGraph.finalAggregate(-3, 3, ts2);
    Assert.assertEquals(merge(partialOutput1, partialOutput2), result2);

    relationGraph.savePartialOutput(3, 4, partialOutput3);
    // [w=4, i=2] final aggregation at time 4
    final Map<Integer, Long> result3 = relationGraph.finalAggregate(0, 4, ts1);
    Assert.assertEquals(merge(partialOutput1, partialOutput2, partialOutput3), result3);
    // [w=10, i=4] final aggregation at time 4
    final Map<Integer, Long> result4 = relationGraph.finalAggregate(-6, 4, ts3);
    Assert.assertEquals(merge(partialOutput1, partialOutput2, partialOutput3), result4);

    relationGraph.savePartialOutput(4, 6, partialOutput4);
    // [w=4, i=2] final aggregation at time 6
    final Map<Integer, Long> result5 = relationGraph.finalAggregate(2, 6, ts1);
    Assert.assertEquals(merge(partialOutput2, partialOutput3, partialOutput4), result5);
    // [w=6, i=3] final aggregation at time 6
    final Map<Integer, Long> result6 = relationGraph.finalAggregate(0, 6, ts2);
    Assert.assertEquals(merge(partialOutput1,
            partialOutput2, partialOutput3, partialOutput4), result6);

    relationGraph.savePartialOutput(6, 8, partialOutput5);
    // [w=4, i=2] final aggregation at time 8
    final Map<Integer, Long> result7 = relationGraph.finalAggregate(4, 8, ts1);
    Assert.assertEquals(merge(partialOutput4, partialOutput5), result7);
    // [w=10, i=4] final aggregation at time 8
    final Map<Integer, Long> result8 = relationGraph.finalAggregate(-2, 8, ts3);
    Assert.assertEquals(merge(partialOutput1,
        partialOutput2, partialOutput3, partialOutput4, partialOutput5), result8);

    relationGraph.savePartialOutput(8, 9, partialOutput6);
    // [w=6, i=3] final aggregation at time 9
    final Map<Integer, Long> result9 = relationGraph.finalAggregate(3, 9, ts2);
    Assert.assertEquals(merge(partialOutput3, partialOutput4,
        partialOutput5, partialOutput6), result9);

    relationGraph.savePartialOutput(9, 10, partialOutput7);
    // [w=4, i=2] final aggregation at time 10
    final Map<Integer, Long> result10 = relationGraph.finalAggregate(6, 10, ts1);
    Assert.assertEquals(merge(partialOutput5, partialOutput6, partialOutput7), result10);

    relationGraph.savePartialOutput(10, 12, partialOutput8);
    // [w=4, i=2] final aggregation at time 12
    final Map<Integer, Long> result11 = relationGraph.finalAggregate(8, 12, ts1);
    Assert.assertEquals(merge(partialOutput6, partialOutput7, partialOutput8), result11);
    // [w=6, i=3] final aggregation at time 12
    final Map<Integer, Long> result12 = relationGraph.finalAggregate(6, 12, ts2);
    Assert.assertEquals(merge(partialOutput5,
        partialOutput6, partialOutput7, partialOutput8), result12);
    // [w=10, i=4] final aggregation at time 12
    final Map<Integer, Long> result13 = relationGraph.finalAggregate(2, 12, ts3);
    Assert.assertEquals(merge(partialOutput2,
        partialOutput3, partialOutput4, partialOutput5,
        partialOutput6, partialOutput7, partialOutput8), result13);

    relationGraph.savePartialOutput(12, 14, partialOutput9);
    // [w=4, i=2] final aggregation at time 14
    final Map<Integer, Long> result14 = relationGraph.finalAggregate(10, 14, ts1);
    Assert.assertEquals(merge(partialOutput8, partialOutput9), result14);

    relationGraph.savePartialOutput(14, 15, partialOutput10);
    // [w=6, i=3] final aggregation at time 15
    final Map<Integer, Long> result15 = relationGraph.finalAggregate(9, 15, ts2);
    Assert.assertEquals(merge(partialOutput7, partialOutput8,
        partialOutput9, partialOutput10), result15);

    relationGraph.savePartialOutput(15, 16, partialOutput11);
    // [w=4, i=2] final aggregation at time 16
    final Map<Integer, Long> result16 = relationGraph.finalAggregate(12, 16, ts1);
    Assert.assertEquals(merge(partialOutput9, partialOutput10, partialOutput11), result16);
    // [w=10, i=4] final aggregation at time 16
    final Map<Integer, Long> result17 = relationGraph.finalAggregate(6, 16, ts3);
    Assert.assertEquals(merge(partialOutput5, partialOutput6, partialOutput7,
        partialOutput8, partialOutput9, partialOutput10, partialOutput11), result17);
  }

  private Map<Integer, Long> merge(Map<Integer, Long> map1, Map<Integer, Long> ... maps) {
    final Map<Integer, Long> result = new HashMap<>();
    result.putAll(map1);

    for (Map<Integer, Long> map2 : maps) {
      for (Map.Entry<Integer, Long> entry : map2.entrySet()) {
        Long oldVal = result.get(entry.getKey());
        if (oldVal == null) {
          oldVal = 0L;
        }
        result.put(entry.getKey(), oldVal + entry.getValue());
      }
    }
    return result;
  }
}
