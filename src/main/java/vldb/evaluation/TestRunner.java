package vldb.evaluation;

import edu.snu.tempest.example.util.writer.OutputWriter;
import org.apache.reef.tang.*;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import vldb.evaluation.common.FileWordGenerator;
import vldb.evaluation.common.Generator;
import vldb.evaluation.parameter.EndTime;
import vldb.evaluation.util.Profiler;
import vldb.example.DefaultExtractor;
import vldb.operator.OutputEmitter;
import vldb.operator.window.aggregator.impl.CountByKeyAggregator;
import vldb.operator.window.aggregator.impl.KeyExtractor;
import vldb.operator.window.timescale.*;
import vldb.operator.window.timescale.common.TimescaleParser;
import vldb.operator.window.timescale.cutty.CuttyMWOConfiguration;
import vldb.operator.window.timescale.flatfit.FlatFitMWOConfiguration;
import vldb.operator.window.timescale.naive.NaiveMWOConfiguration;
import vldb.operator.window.timescale.onthefly.OntheflyMWOConfiguration;
import vldb.operator.window.timescale.pafas.*;
import vldb.operator.window.timescale.pafas.active.*;
import vldb.operator.window.timescale.pafas.dynamic.DynamicDPOutputLookupTableImpl;
import vldb.operator.window.timescale.pafas.dynamic.DynamicOptimizedDependencyGraphImpl;
import vldb.operator.window.timescale.pafas.event.WindowTimeEvent;
import vldb.operator.window.timescale.pafas.vldb2018.FastCuttyCombinedDependencyGraph;
import vldb.operator.window.timescale.pafas.vldb2018.FastFitDPSelectionAlgorithm;
import vldb.operator.window.timescale.pafas.vldb2018.FlatFitCombinedDependencyGraph;
import vldb.operator.window.timescale.pafas.vldb2018.FlatFitCombinedMWOConfiguration;
import vldb.operator.window.timescale.pafas.vldb2018.multithread.MultiThreadImprovedFinalAggregator;
import vldb.operator.window.timescale.pafas.vldb2018.multithread.SimpleTreeHeightDPSelectionAlgorithm;
import vldb.operator.window.timescale.pafas.vldb2018.multithread.SimpleTreeHeightDependencyGraph;
import vldb.operator.window.timescale.pafas.vldb2018.singlethread.MultiThreadFinalAggregator;
import vldb.operator.window.timescale.pafas.vldb2018.singlethread.SingleThreadFinalAggregator;
import vldb.operator.window.timescale.parameter.*;
import vldb.operator.window.timescale.profiler.AggregationCounter;
import vldb.operator.window.timescale.triops.TriOpsMWOConfiguration;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by taegeonum on 4/28/16.
 */
public final class TestRunner {
  //static final long totalTime = 1800;

  public enum OperatorType {
    FastSt, // fast static
    FastDy, // fast dynamic
    FastSm, // fast static memory
    FastInter, // fast intermediate aggregate by adjusting partials
    FastEg, // fast eager agg
    FastPruning, // fast pruning after DP
    FastRb, // fast rebuild
    FastRb2, // fast rebuild with weight
    FastRbNum, // fast rebuild with num limited num
    NoOverlap, // no overlap
    FastOverlap, // rm overlapping windows
    FastRandom, // random selection
    FastWeight, // fast pruning while updating weight
    OTFSta,
    OTFDyn,
    FastAc,
    TriOps,
    Naivee,
    Cuttyy,
    FltFit,
    FastFit,
    CuttyyP, // cutty parallel
    FastFitP, // fast fit parallel
    FastH, // fast tree height threshold
    FastHS, // fast tree height threshold in a single thread
    FastCutty,
  }

  @NamedParameter(short_name="window_change_period", default_value = "10")
  public static class WindowChangePeriod implements Name<Integer> {}

  private static Configuration getOperatorConf(final OperatorType operatorType,
                                               final String timescaleString) {
    switch (operatorType) {
      case FastSt:
        return StaticSingleMWOConfiguration.CONF
            .set(StaticSingleMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
            .set(StaticSingleMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
            .set(StaticSingleMWOConfiguration.SELECTION_ALGORITHM, ActiveDPSelectionAlgorithm.class)
            .set(StaticSingleMWOConfiguration.OUTPUT_LOOKUP_TABLE, DPOutputLookupTableImpl.class)
            .set(StaticSingleMWOConfiguration.DEPENDENCY_GRAPH, StaticDependencyGraphImpl.class)
            .set(StaticSingleMWOConfiguration.START_TIME, "0")
            .build();
      case FastCutty:
        return FlatFitCombinedMWOConfiguration.CONF
            .set(FlatFitCombinedMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
            .set(FlatFitCombinedMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
            .set(FlatFitCombinedMWOConfiguration.SELECTION_ALGORITHM, FastFitDPSelectionAlgorithm.class)
            .set(FlatFitCombinedMWOConfiguration.OUTPUT_LOOKUP_TABLE, DPOutputLookupTableImpl.class)
            .set(FlatFitCombinedMWOConfiguration.DEPENDENCY_GRAPH, FastCuttyCombinedDependencyGraph.class)
            .set(FlatFitCombinedMWOConfiguration.FINAL_AGGREGATOR, SingleThreadFinalAggregator.class)
            .set(FlatFitCombinedMWOConfiguration.START_TIME, "0")
            .build();
      case FastFit:
        return FlatFitCombinedMWOConfiguration.CONF
            .set(FlatFitCombinedMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
            .set(FlatFitCombinedMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
            .set(FlatFitCombinedMWOConfiguration.SELECTION_ALGORITHM, FastFitDPSelectionAlgorithm.class)
            .set(FlatFitCombinedMWOConfiguration.OUTPUT_LOOKUP_TABLE, DPOutputLookupTableImpl.class)
            .set(FlatFitCombinedMWOConfiguration.DEPENDENCY_GRAPH, FlatFitCombinedDependencyGraph.class)
            .set(FlatFitCombinedMWOConfiguration.FINAL_AGGREGATOR, SingleThreadFinalAggregator.class)
            .set(FlatFitCombinedMWOConfiguration.START_TIME, "0")
            .build();
      case FastH:
        return FlatFitCombinedMWOConfiguration.CONF
            .set(FlatFitCombinedMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
            .set(FlatFitCombinedMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
            .set(FlatFitCombinedMWOConfiguration.SELECTION_ALGORITHM, SimpleTreeHeightDPSelectionAlgorithm.class)
            .set(FlatFitCombinedMWOConfiguration.OUTPUT_LOOKUP_TABLE, DPOutputLookupTableImpl.class)
            .set(FlatFitCombinedMWOConfiguration.DEPENDENCY_GRAPH, SimpleTreeHeightDependencyGraph.class)
            .set(FlatFitCombinedMWOConfiguration.FINAL_AGGREGATOR, MultiThreadImprovedFinalAggregator.class)
            .set(FlatFitCombinedMWOConfiguration.START_TIME, "0")
            .build();
      case FastHS:
        return FlatFitCombinedMWOConfiguration.CONF
            .set(FlatFitCombinedMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
            .set(FlatFitCombinedMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
            .set(FlatFitCombinedMWOConfiguration.SELECTION_ALGORITHM, SimpleTreeHeightDPSelectionAlgorithm.class)
            .set(FlatFitCombinedMWOConfiguration.OUTPUT_LOOKUP_TABLE, DPOutputLookupTableImpl.class)
            .set(FlatFitCombinedMWOConfiguration.DEPENDENCY_GRAPH, SimpleTreeHeightDependencyGraph.class)
            .set(FlatFitCombinedMWOConfiguration.FINAL_AGGREGATOR, SingleThreadFinalAggregator.class)
            .set(FlatFitCombinedMWOConfiguration.START_TIME, "0")
            .build();
      case FastFitP:
        return FlatFitCombinedMWOConfiguration.CONF
            .set(FlatFitCombinedMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
            .set(FlatFitCombinedMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
            .set(FlatFitCombinedMWOConfiguration.SELECTION_ALGORITHM, FastFitDPSelectionAlgorithm.class)
            .set(FlatFitCombinedMWOConfiguration.OUTPUT_LOOKUP_TABLE, DPOutputLookupTableImpl.class)
            .set(FlatFitCombinedMWOConfiguration.DEPENDENCY_GRAPH, FlatFitCombinedDependencyGraph.class)
            .set(FlatFitCombinedMWOConfiguration.FINAL_AGGREGATOR, MultiThreadImprovedFinalAggregator.class)
            .set(FlatFitCombinedMWOConfiguration.START_TIME, "0")
            .build();
      case CuttyyP:
        return FlatFitCombinedMWOConfiguration.CONF
            .set(FlatFitCombinedMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
            .set(FlatFitCombinedMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
            .set(FlatFitCombinedMWOConfiguration.SELECTION_ALGORITHM, FastFitDPSelectionAlgorithm.class)
            .set(FlatFitCombinedMWOConfiguration.OUTPUT_LOOKUP_TABLE, DPOutputLookupTableImpl.class)
            .set(FlatFitCombinedMWOConfiguration.DEPENDENCY_GRAPH, FastCuttyCombinedDependencyGraph.class)
            .set(FlatFitCombinedMWOConfiguration.FINAL_AGGREGATOR, MultiThreadImprovedFinalAggregator.class)
            .set(FlatFitCombinedMWOConfiguration.START_TIME, "0")
            .build();
      case FastAc:
        return StaticActiveSingleMWOConfiguration.CONF
            .set(StaticActiveSingleMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
            .set(StaticActiveSingleMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
            .set(StaticActiveSingleMWOConfiguration.SELECTION_ALGORITHM, ActiveDPSelectionAlgorithm.class)
            .set(StaticActiveSingleMWOConfiguration.OUTPUT_LOOKUP_TABLE, DPOutputLookupTableImpl.class)
            .set(StaticActiveSingleMWOConfiguration.DEPENDENCY_GRAPH, AdjustPartialDependencyGraph.class)
            .set(StaticActiveSingleMWOConfiguration.START_TIME, "0")
            .build();
      case FastWeight:
        return StaticSingleMWOConfiguration.CONF
            .set(StaticSingleMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
            .set(StaticSingleMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
            .set(StaticSingleMWOConfiguration.SELECTION_ALGORITHM, ActiveDPSelectionAlgorithm.class)
            .set(StaticSingleMWOConfiguration.OUTPUT_LOOKUP_TABLE, DPOutputLookupTableImpl.class)
            .set(StaticSingleMWOConfiguration.DEPENDENCY_GRAPH, PruningParallelMaxDependencyGraphImpl.class)
            .set(StaticSingleMWOConfiguration.START_TIME, "0")
            .build();
      case FastRandom:
        return StaticSingleMWOConfiguration.CONF
            .set(StaticSingleMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
            .set(StaticSingleMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
            .set(StaticSingleMWOConfiguration.SELECTION_ALGORITHM, ActiveDPSelectionAlgorithm.class)
            .set(StaticSingleMWOConfiguration.OUTPUT_LOOKUP_TABLE, DPOutputLookupTableImpl.class)
            .set(StaticSingleMWOConfiguration.DEPENDENCY_GRAPH, RandomSelectionDependencyGraphImpl.class)
            .set(StaticSingleMWOConfiguration.START_TIME, "0")
            .build();
      case FastPruning:
        return StaticSingleMWOConfiguration.CONF
            .set(StaticSingleMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
            .set(StaticSingleMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
            .set(StaticSingleMWOConfiguration.SELECTION_ALGORITHM, ActiveDPSelectionAlgorithm.class)
            .set(StaticSingleMWOConfiguration.OUTPUT_LOOKUP_TABLE, DPOutputLookupTableImpl.class)
            .set(StaticSingleMWOConfiguration.DEPENDENCY_GRAPH, FineGrainedPruningDependencyGraphImpl.class)
            .set(StaticSingleMWOConfiguration.START_TIME, "0")
            .build();
      case FastRb:
        return StaticSingleMWOConfiguration.CONF
            .set(StaticSingleMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
            .set(StaticSingleMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
            .set(StaticSingleMWOConfiguration.SELECTION_ALGORITHM, ActiveDPSelectionAlgorithm.class)
            .set(StaticSingleMWOConfiguration.OUTPUT_LOOKUP_TABLE, DPOutputLookupTableImpl.class)
            .set(StaticSingleMWOConfiguration.DEPENDENCY_GRAPH, FineGrainedPruningRebuildDependencyGraphImpl.class)
            .set(StaticSingleMWOConfiguration.START_TIME, "0")
            .build();
      case FastRb2:
        return StaticSingleMWOConfiguration.CONF
            .set(StaticSingleMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
            .set(StaticSingleMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
            .set(StaticSingleMWOConfiguration.SELECTION_ALGORITHM, ActiveDPSelectionAlgorithm.class)
            .set(StaticSingleMWOConfiguration.OUTPUT_LOOKUP_TABLE, DPOutputLookupTableImpl.class)
            .set(StaticSingleMWOConfiguration.DEPENDENCY_GRAPH, FineGrainedPruningRebuildDependencyGraphImpl.class)
            .set(StaticSingleMWOConfiguration.START_TIME, "0")
            .build();
      case FastRbNum:
        return StaticSingleMWOConfiguration.CONF
            .set(StaticSingleMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
            .set(StaticSingleMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
            .set(StaticSingleMWOConfiguration.SELECTION_ALGORITHM, ActiveDPSelectionAlgorithm.class)
            .set(StaticSingleMWOConfiguration.OUTPUT_LOOKUP_TABLE, DPOutputLookupTableImpl.class)
            .set(StaticSingleMWOConfiguration.DEPENDENCY_GRAPH, FineGrainedPruningRebuildDependencyGraphImpl.class)
            .set(StaticSingleMWOConfiguration.START_TIME, "0")
            .build();
      case FastOverlap:
      case NoOverlap:
        return StaticSingleMWOConfiguration.CONF
            .set(StaticSingleMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
            .set(StaticSingleMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
            .set(StaticSingleMWOConfiguration.SELECTION_ALGORITHM, ActiveDPSelectionAlgorithm.class)
            .set(StaticSingleMWOConfiguration.OUTPUT_LOOKUP_TABLE, DPOutputLookupTableImpl.class)
            .set(StaticSingleMWOConfiguration.DEPENDENCY_GRAPH, WindowFineGrainedPruningDependencyGraphImpl.class)
            .set(StaticSingleMWOConfiguration.START_TIME, "0")
            .build();
      case FastEg:
        return EagerMWOConfiguration.CONF
            .set(EagerMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
            .set(EagerMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
            .set(EagerMWOConfiguration.SELECTION_ALGORITHM, ActiveDPSelectionAlgorithm.class)
            .set(EagerMWOConfiguration.OUTPUT_LOOKUP_TABLE, DPOutputLookupTableImpl.class)
            .set(EagerMWOConfiguration.START_TIME, "0")
            .build();
      case FastDy:
        return ActiveDynamicMWOConfiguration.CONF
            .set(ActiveDynamicMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
            .set(ActiveDynamicMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
            .set(ActiveDynamicMWOConfiguration.SELECTION_ALGORITHM, DynamicDPTradeOffSelectionAlgorithm.class)
            .set(ActiveDynamicMWOConfiguration.OUTPUT_LOOKUP_TABLE, DynamicDPOutputLookupTableImpl.class)
            .set(ActiveDynamicMWOConfiguration.DYNAMIC_DEPENDENCY, DynamicOptimizedDependencyGraphImpl.class)
            .set(ActiveDynamicMWOConfiguration.START_TIME, "0")
            .build();
      case FastSm:
        throw new RuntimeException("not supported yet");
      case OTFSta:
        return OntheflyMWOConfiguration.STATIC_CONF
          .set(OntheflyMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
          .set(OntheflyMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
          .set(OntheflyMWOConfiguration.START_TIME, "0")
          .build();
      case Cuttyy:
        return CuttyMWOConfiguration.CONF
            .set(CuttyMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
            .set(CuttyMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
            .set(CuttyMWOConfiguration.START_TIME, "0")
            .build();
      case FltFit:
        return FlatFitMWOConfiguration.CONF
            .set(FlatFitMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
            .set(FlatFitMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
            .set(FlatFitMWOConfiguration.START_TIME, "0")
            .build();
      case TriOps:
        // TriOPs
        return TriOpsMWOConfiguration.CONF
            .set(TriOpsMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
            .set(TriOpsMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
            .set(TriOpsMWOConfiguration.START_TIME, "0")
            .build();
      case Naivee:
        return NaiveMWOConfiguration.CONF
            .set(NaiveMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
            .set(NaiveMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
            .set(NaiveMWOConfiguration.START_TIME, "0")
            .build();
      /*
      case DYNAMIC_DP:
        return DynamicMWOConfiguration.CONF
          .set(DynamicMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
          .set(DynamicMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
            .set(DynamicMWOConfiguration.SELECTION_ALGORITHM, DynamicDPSelectionAlgorithm.class)
            .set(DynamicMWOConfiguration.OUTPUT_LOOKUP_TABLE, DynamicDPOutputLookupTableImpl.class)
            .set(DynamicMWOConfiguration.DYNAMIC_DEPENDENCY, DynamicOptimizedDependencyGraphImpl.class)
            .set(DynamicMWOConfiguration.DYNAMIC_PARTIAL, DynamicOptimizedPartialTimespans.class)
          .set(DynamicMWOConfiguration.START_TIME, "0")
          .build();
      case SCALABLE_DP:
        return MultiThreadDynamicMWOConfiguration.CONF
            .set(MultiThreadDynamicMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
            .set(MultiThreadDynamicMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
            .set(MultiThreadDynamicMWOConfiguration.SELECTION_ALGORITHM, DynamicDPSelectionAlgorithm.class)
            .set(MultiThreadDynamicMWOConfiguration.OUTPUT_LOOKUP_TABLE, DynamicDPOutputLookupTableImpl.class)
            .set(MultiThreadDynamicMWOConfiguration.DYNAMIC_DEPENDENCY, DynamicOptimizedDependencyGraphImpl.class)
            .set(MultiThreadDynamicMWOConfiguration.DYNAMIC_PARTIAL, DynamicOptimizedPartialTimespans.class)
            .set(MultiThreadDynamicMWOConfiguration.START_TIME, "0")
            .build();
      case SCALABLE_GREEDY:
        return MultiThreadDynamicMWOConfiguration.CONF
            .set(MultiThreadDynamicMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
            .set(MultiThreadDynamicMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
            .set(MultiThreadDynamicMWOConfiguration.SELECTION_ALGORITHM, DynamicGreedySelectionAlgorithm.class)
            .set(MultiThreadDynamicMWOConfiguration.OUTPUT_LOOKUP_TABLE, DynamicGreedyOutputLookupTableImpl.class)
            .set(MultiThreadDynamicMWOConfiguration.DYNAMIC_DEPENDENCY, DynamicOptimizedDependencyGraphImpl.class)
            .set(MultiThreadDynamicMWOConfiguration.DYNAMIC_PARTIAL, DynamicOptimizedPartialTimespans.class)
            .set(MultiThreadDynamicMWOConfiguration.START_TIME, "0")
            .build();
      case DYNAMIC_ADAPTIVE:
        return DynamicMWOConfiguration.CONF
            .set(DynamicMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
            .set(DynamicMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
            .set(DynamicMWOConfiguration.SELECTION_ALGORITHM, DynamicDPSelectionAlgorithm.class)
            .set(DynamicMWOConfiguration.OUTPUT_LOOKUP_TABLE, DynamicGreedyOutputLookupTableImpl.class)
            .set(DynamicMWOConfiguration.DYNAMIC_DEPENDENCY, DynamicAdaptiveOptimizedDependencyGraphImpl.class)
            .set(DynamicMWOConfiguration.DYNAMIC_PARTIAL, DynamicOptimizedPartialTimespans.class)
            .set(DynamicMWOConfiguration.START_TIME, "0")
            .build();
      case DYNAMIC_GREEDY:
        return DynamicMWOConfiguration.CONF
            .set(DynamicMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
            .set(DynamicMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
            .set(DynamicMWOConfiguration.SELECTION_ALGORITHM, DynamicGreedySelectionAlgorithm.class)
            .set(DynamicMWOConfiguration.OUTPUT_LOOKUP_TABLE, DynamicGreedyOutputLookupTableImpl.class)
            .set(DynamicMWOConfiguration.DYNAMIC_DEPENDENCY, DynamicOptimizedDependencyGraphImpl.class)
            .set(DynamicMWOConfiguration.DYNAMIC_PARTIAL, DynamicOptimizedPartialTimespans.class)
            .set(DynamicMWOConfiguration.START_TIME, "0")
            .build();
      case DYNAMIC_ALL_STORE:
        return DynamicAllStoreMWOConfiguration.CONF
            .set(DynamicAllStoreMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
            .set(DynamicAllStoreMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
            .set(DynamicAllStoreMWOConfiguration.SELECTION_ALGORITHM, DynamicGreedySelectionAlgorithm.class)
            .set(DynamicAllStoreMWOConfiguration.OUTPUT_LOOKUP_TABLE, DynamicGreedyOutputLookupTableImpl.class)
            .set(DynamicAllStoreMWOConfiguration.DYNAMIC_DEPENDENCY, DynamicOptimizedDependencyGraphImpl.class)
            .set(DynamicAllStoreMWOConfiguration.DYNAMIC_PARTIAL, DynamicOptimizedPartialTimespans.class)
            .set(DynamicAllStoreMWOConfiguration.START_TIME, "0")
            .build();
      case PAFAS_SINGLE:
        return StaticSingleMWOConfiguration.CONF
            .set(StaticSingleMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
            .set(StaticSingleMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
            .set(StaticSingleMWOConfiguration.SELECTION_ALGORITHM, ActiveDPSelectionAlgorithm.class)
            .set(StaticSingleMWOConfiguration.OUTPUT_LOOKUP_TABLE, DPOutputLookupTableImpl.class)
            .set(StaticSingleMWOConfiguration.START_TIME, "0")
            .build();
      case PAFAS_SINGLE_ALL_STORE:
        return StaticSingleAllStoreMWOConfiguration.CONF
            .set(StaticSingleAllStoreMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
            .set(StaticSingleAllStoreMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
            .set(StaticSingleAllStoreMWOConfiguration.SELECTION_ALGORITHM, ActiveDPSelectionAlgorithm.class)
            .set(StaticSingleAllStoreMWOConfiguration.OUTPUT_LOOKUP_TABLE, DPOutputLookupTableImpl.class)
            .set(StaticSingleAllStoreMWOConfiguration.START_TIME, "0")
            .build();
      case PAFAS_SINGLE_NO_REFCNT:
        return StaticSingleNoRefCntMWOConfiguration.CONF
            .set(StaticSingleNoRefCntMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
            .set(StaticSingleNoRefCntMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
            .set(StaticSingleNoRefCntMWOConfiguration.SELECTION_ALGORITHM, ActiveDPSelectionAlgorithm.class)
            .set(StaticSingleNoRefCntMWOConfiguration.OUTPUT_LOOKUP_TABLE, DPOutputLookupTableImpl.class)
            .set(StaticSingleNoRefCntMWOConfiguration.START_TIME, "0")
            .build();
      case PAFAS_SINGLE_GREEDY:
        return StaticSingleMWOConfiguration.CONF
            .set(StaticSingleMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
            .set(StaticSingleMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
            .set(StaticSingleMWOConfiguration.SELECTION_ALGORITHM, GreedySelectionAlgorithm.class)
            .set(StaticSingleMWOConfiguration.OUTPUT_LOOKUP_TABLE, DefaultOutputLookupTableImpl.class)
            .set(StaticSingleMWOConfiguration.START_TIME, "0")
            .build();
      case PAFAS:
        // PAFAS-Greedy
        return StaticMWOConfiguration.CONF
            .set(StaticMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
            .set(StaticMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
            .set(StaticMWOConfiguration.SELECTION_ALGORITHM, GreedySelectionAlgorithm.class)
            .set(StaticMWOConfiguration.OUTPUT_LOOKUP_TABLE, DefaultOutputLookupTableImpl.class)
            .set(StaticMWOConfiguration.START_TIME, "0")
            .build();
      case OnTheFly:
        // On-the-fly operator
        return OntheflyMWOConfiguration.CONF
            .set(OntheflyMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
            .set(OntheflyMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
            .set(OntheflyMWOConfiguration.START_TIME, "0")
            .build();
      case TriOps:
        // TriOPs
        return TriOpsMWOConfiguration.CONF
            .set(TriOpsMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
            .set(TriOpsMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
            .set(TriOpsMWOConfiguration.START_TIME, "0")
            .build();
      case Naive:
        return NaiveMWOConfiguration.CONF
            .set(NaiveMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
            .set(NaiveMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
            .set(NaiveMWOConfiguration.START_TIME, "0")
            .build();
            */
      default:
        throw new RuntimeException("Not implemented Naive");
    }
  }


  public static Result runFileWordDynamicTest(final String timescaleString,
                                       final int numThreads,
                                       final String filePath,
                                       final OperatorType operatorType,
                                       final double inputRate,
                                       final long totalTime,
                                       final OutputWriter writer,
                                       final String prefix,
                                       final int windowChangePeriod) throws Exception {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindImplementation(KeyExtractor.class, DefaultExtractor.class);
    jcb.bindNamedParameter(NumThreads.class, numThreads+"");
    jcb.bindNamedParameter(FileWordGenerator.FileDataPath.class, filePath);
    jcb.bindNamedParameter(EndTime.class, totalTime+"");

    final List<Timescale> timescales = TimescaleParser.parseFromStringNoSort(timescaleString);
    //Collections.sort(timescales);

    /*
    writer.writeLine(outputPath+"/result", "-------------------------------------");
    writer.writeLine(outputPath+"/result", "@NumWindows=" + timescales.size());
    writer.writeLine(outputPath+"/result", "@Windows="+timescaleString);
    writer.writeLine(outputPath+"/result", "-------------------------------------");
    */
    int i = 0;
    Configuration conf = null;
    /*
    if (operatorType == OperatorType.DYNAMIC_WINDOW_GREEDY) {
      conf = MultiThreadDynamicMWOConfiguration.CONF
          .set(MultiThreadDynamicMWOConfiguration.INITIAL_TIMESCALES, timescales.get(0).toString())
          .set(MultiThreadDynamicMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
          .set(MultiThreadDynamicMWOConfiguration.DYNAMIC_DEPENDENCY, DynamicOptimizedDependencyGraphImpl.class)
          .set(MultiThreadDynamicMWOConfiguration.OUTPUT_LOOKUP_TABLE, DynamicGreedyOutputLookupTableImpl.class)
          .set(MultiThreadDynamicMWOConfiguration.SELECTION_ALGORITHM, DynamicGreedySelectionAlgorithm.class)
          .set(MultiThreadDynamicMWOConfiguration.DYNAMIC_PARTIAL, DynamicOptimizedPartialTimespans.class)
          .set(MultiThreadDynamicMWOConfiguration.START_TIME, 0)
          .build();
    } else if (operatorType == OperatorType.DYNAMIC_WINDOW_DP) {
      conf = MultiThreadDynamicMWOConfiguration.CONF
          .set(MultiThreadDynamicMWOConfiguration.INITIAL_TIMESCALES, timescales.get(0).toString())
          .set(MultiThreadDynamicMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
          .set(MultiThreadDynamicMWOConfiguration.DYNAMIC_DEPENDENCY, DynamicOptimizedDependencyGraphImpl.class)
          .set(MultiThreadDynamicMWOConfiguration.OUTPUT_LOOKUP_TABLE, DynamicDPOutputLookupTableImpl.class)
          .set(MultiThreadDynamicMWOConfiguration.SELECTION_ALGORITHM, DynamicDPSelectionAlgorithm.class)
          .set(MultiThreadDynamicMWOConfiguration.DYNAMIC_PARTIAL, DynamicOptimizedPartialTimespans.class)
          .set(MultiThreadDynamicMWOConfiguration.START_TIME, 0)
          .build();
    } else if (operatorType == OperatorType.DYNAMIC_WINDOW_DP_SMALLADD) {
      conf = MultiThreadDynamicMWOConfiguration.CONF
          .set(MultiThreadDynamicMWOConfiguration.INITIAL_TIMESCALES, timescales.get(0).toString())
          .set(MultiThreadDynamicMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
          .set(MultiThreadDynamicMWOConfiguration.DYNAMIC_DEPENDENCY, DynamicSmallCostAddDependencyGraphImpl.class)
          .set(MultiThreadDynamicMWOConfiguration.OUTPUT_LOOKUP_TABLE, DynamicDPOutputLookupTableImpl.class)
          .set(MultiThreadDynamicMWOConfiguration.SELECTION_ALGORITHM, DynamicDPSelectionAlgorithm.class)
          .set(MultiThreadDynamicMWOConfiguration.DYNAMIC_PARTIAL, DynamicOptimizedPartialTimespans.class)
          .set(MultiThreadDynamicMWOConfiguration.START_TIME, 0)
          .build();
    } else if (operatorType == OperatorType.DYNAMIC_OnTheFly) {
      conf = OntheflyMWOConfiguration.CONF
          .set(OntheflyMWOConfiguration.INITIAL_TIMESCALES, timescales.get(0).toString())
          .set(OntheflyMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
          .set(OntheflyMWOConfiguration.START_TIME, 0)
          .build();
    } else if (operatorType == OperatorType.DYNAMIC_NAIVE) {
      conf = NaiveMWOConfiguration.CONF
          .set(NaiveMWOConfiguration.INITIAL_TIMESCALES, timescales.get(0).toString())
          .set(NaiveMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
          .set(NaiveMWOConfiguration.START_TIME, 0)
          .build();
    }
    */

    //writer.writeLine(outputPath+"/result", "=================\nOperator=" + operatorType.name());
    final Injector newInjector = Tang.Factory.getTang().newInjector(Configurations.merge(jcb.build(), conf));
    final Generator wordGenerator = newInjector.getInstance(FileWordGenerator.class);

    final Lock lock = new ReentrantLock();
    final Condition canProceed = lock.newCondition();
    final AtomicLong tickTime = new AtomicLong(0);

    newInjector.bindVolatileInstance(TimeWindowOutputHandler.class,
        new EvaluationHandler<>(operatorType.name(), null, totalTime, writer, prefix,
            tickTime, lock, null, canProceed));
    final TimescaleWindowOperator<Object, Map<String, Long>> mwo = newInjector.getInstance(TimescaleWindowOperator.class);
    final AggregationCounter aggregationCounter = newInjector.getInstance(AggregationCounter.class);
    final TimeMonitor timeMonitor = newInjector.getInstance(TimeMonitor.class);
    final PeriodCalculator periodCalculator = newInjector.getInstance(PeriodCalculator.class);
    final long period = periodCalculator.getPeriod();
    i += 1;


    final long currTime = System.currentTimeMillis();
    long processedInput = 1;
    //Thread.sleep(period);
    // FOR TIME
    //while (processedInput < inputRate * (totalTime)) {
    // FOR COUNT
    long tick = 1;

    long currInputRate = (long)inputRate;
    Scanner poissonFile = null;
    if (inputRate == 0) {
      poissonFile = new Scanner(new File("./dataset/poisson.txt"));
      currInputRate = Long.valueOf(poissonFile.nextLine());
    }

    long currInput = 0;
    double rebuildingTime = 0;
    while (tick <= windowChangePeriod * timescales.size() * 2) {
      //System.out.println("totalTime: " + (totalTime*1000) + ", elapsed: " + (System.currentTimeMillis() - currTime));
      final String word = wordGenerator.nextString();
      final long cTime = System.nanoTime();
      //System.out.println("word: " + word);
      if (word == null) {
        // End of input
        break;
      }

      mwo.execute(word);
      currInput += 1;

      if (currInput >= currInputRate) {
        mwo.execute(new WindowTimeEvent(tick));
        // Add window
        if (tick < timescales.size() * windowChangePeriod && tick % windowChangePeriod == 0) {
          final int index = (int) tick / windowChangePeriod;
          final Timescale ts = timescales.get(index);
          System.out.println("ADD: " + ts + ", " + tick);
          final long addStartTime = System.nanoTime();
          mwo.addWindow(ts, tick);
          final long addEndTime = System.nanoTime();
          final double elapsed = (addEndTime - addStartTime)/1000000.0;
          rebuildingTime += elapsed;
          writer.writeLine(prefix + "_result", "ADD\t" + ts + "\t" + elapsed);
        }

        if (tick > timescales.size() * windowChangePeriod && tick < timescales.size() * 2 * windowChangePeriod && tick % windowChangePeriod == 0) {
          int index = ((int) (tick / windowChangePeriod) % timescales.size());
          final Timescale ts = timescales.get(index);
          System.out.println("RM: " + ts + ", " + tick);
          final long rmStartTime = System.nanoTime();
          mwo.removeWindow(ts, tick);
          final long rmEndTime = System.nanoTime();
          final double elapsed = (rmEndTime - rmStartTime)/1000000.0;
          rebuildingTime += elapsed;
          writer.writeLine(prefix + "_result", "REMOVE\t" + ts + "\t" + elapsed);
        }
        tick += 1;
        currInput = 0;
        if (inputRate == 0) {
          currInputRate = Long.valueOf(poissonFile.nextLine());
        }
      }

      processedInput += 1;
      //while (System.nanoTime() - cTime < 1000000000*(1.0/inputRate)) {
      // adjust input rate
      //}
    }
    final long endTime = System.currentTimeMillis();
    mwo.close();
    wordGenerator.close();
    final long partialCount = aggregationCounter.getNumPartialAggregation();
    final long finalCount = aggregationCounter.getNumFinalAggregation();
    writer.writeLine(prefix + "_result", "REBUILDING_TIME\t" + rebuildingTime);
    //writer.writeLine(outputPath+"/result", "PARTIAL="+partialCount);
    //writer.writeLine(outputPath+"/result", "FINAL=" + finalCount);
    //writer.writeLine(outputPath+"/result", "ELAPSED_TIME="+(endTime-currTime));
    return new Result(partialCount, finalCount, processedInput, (endTime - currTime), timeMonitor);
  }


  public static Metrics runFileWordTest(final List<Timescale> timescales,
                                       final int numThreads,
                                       final String filePath,
                                       final OperatorType operatorType,
                                       final double inputRate,
                                       final long totalTime,
                                       final OutputWriter writer,
                                       final String prefix,
                                       final double reusingRatio,
                                       final int windowGap,
                                       final int sharedFinalNum,
                                       final double overlappingRatio,
                                       final int threshold) throws Exception {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindImplementation(KeyExtractor.class, DefaultExtractor.class);
    jcb.bindNamedParameter(NumThreads.class, numThreads+"");
    jcb.bindNamedParameter(FileWordGenerator.FileDataPath.class, filePath);
    jcb.bindNamedParameter(EndTime.class, totalTime+"");
    jcb.bindNamedParameter(ReusingRatio.class, reusingRatio+"");
    jcb.bindNamedParameter(WindowGap.class, windowGap+"");
    jcb.bindNamedParameter(SharedFinalNum.class, sharedFinalNum+"");
    jcb.bindNamedParameter(OverlappingRatio.class, overlappingRatio + "");
    jcb.bindNamedParameter(MultiThreadFinalAggregator.ParallelThreshold.class, threshold + "");

    Collections.sort(timescales);

    long totalOutputSize = 0;

    int numOutputs = 0;
    for (final Timescale ts : timescales) {
      numOutputs += (totalTime / ts.intervalSize);
      totalOutputSize += (totalTime / ts.intervalSize) * ts.windowSize;
      for (long etime = ts.intervalSize; etime < ts.windowSize; etime += ts.intervalSize) {
        totalOutputSize += (etime - ts.windowSize);
      }
    }

    final Map<Long, AtomicInteger> outputCount = new HashMap<>();
    for (long i = 1; i <= totalTime; i++) {
      int count = 0;
      for (final Timescale ts : timescales) {
        if (i % ts.intervalSize == 0) {
          count += 1;
        }
      }
      outputCount.put(i, new AtomicInteger(count));
    }

    writer.writeLine(prefix + "_num_outputs", numOutputs+"\t" + totalOutputSize);
    final CountDownLatch countDownLatch = new CountDownLatch(numOutputs);

    final String timescaleString = TimescaleParser.parseToString(timescales);
    /*
    writer.writeLine(outputPath+"/result", "-------------------------------------");
    writer.writeLine(outputPath+"/result", "@NumWindows=" + timescales.size());
    writer.writeLine(outputPath+"/result", "@Windows="+timescaleString);
    writer.writeLine(outputPath+"/result", "-------------------------------------");
    */
    int i = 0;
    final Configuration conf = getOperatorConf(operatorType, timescaleString);

    //writer.writeLine(outputPath+"/result", "=================\nOperator=" + operatorType.name());
    final Injector newInjector = Tang.Factory.getTang().newInjector(Configurations.merge(jcb.build(), conf));
    final Generator wordGenerator = newInjector.getInstance(FileWordGenerator.class);

    final Lock lock = new ReentrantLock();
    final Condition canProceed = lock.newCondition();
    final AtomicLong processedTickTime = new AtomicLong(0);

    newInjector.bindVolatileInstance(AtomicLong.class, processedTickTime);

    newInjector.bindVolatileInstance(TimeWindowOutputHandler.class,
        new EvaluationHandler<>(operatorType.name(), countDownLatch, totalTime, writer, prefix,
            processedTickTime, lock, outputCount, canProceed));

    final TimeMonitor timeMonitor = newInjector.getInstance(TimeMonitor.class);
    final long bst = System.nanoTime();
    final TimescaleWindowOperator<Object, Map<String, Long>> mwo = newInjector.getInstance(TimescaleWindowOperator.class);
    final long bet = System.nanoTime();

    timeMonitor.staticBuildingTime = (bet - bst);

    final PeriodCalculator periodCalculator = newInjector.getInstance(PeriodCalculator.class);
    final Metrics metrics = newInjector.getInstance(Metrics.class);
    final long period = periodCalculator.getPeriod();
    i += 1;

    long processedInput = 1;
    //Thread.sleep(period);
    // FOR TIME
    //while (processedInput < inputRate * (totalTime)) {
    // FOR COUNT
    long tick = 1;

    long currInputRate = (long)inputRate;
    Scanner poissonFile = null;
    if (inputRate == 0) {
       poissonFile = new Scanner(new File("./dataset/poisson.txt"));
      currInputRate = Long.valueOf(poissonFile.nextLine());
    }

    final long currTime = System.currentTimeMillis();
    long currInput = 0;

    while (tick <= totalTime) {

      //System.out.println("totalTime: " + (totalTime*1000) + ", elapsed: " + (System.currentTimeMillis() - currTime));
      final String word = wordGenerator.nextString();
      final long cTime = System.nanoTime();
      //System.out.println("word: " + word);
      if (word == null) {
        // End of input
        break;
      }

      mwo.execute(word);
      currInput += 1;

      if (currInput >= currInputRate) {
        mwo.execute(new WindowTimeEvent(tick));

        // Wait until all outputs are generated
        /*
        lock.lock();
        while (outputCount.get(tick).get() > 0) {
          canProceed.await();
        }
        lock.unlock();
         */

        System.out.println("TICK: " + tick + ", processedTickTime: " + processedTickTime.get());

        final StringBuilder sb1 = new StringBuilder();
        sb1.append(System.currentTimeMillis()); sb1.append("\t"); sb1.append(Profiler.getMemoryUsage()); sb1.append("\t");
        sb1.append(metrics.storedPartial);
        sb1.append("\t");
        sb1.append(metrics.storedFinal);
        sb1.append("\t");
        sb1.append(metrics.storedInter);
        sb1.append("\t");
        sb1.append(metrics.storedPartial + metrics.storedInter + metrics.storedFinal);
        writer.writeLine(prefix + "_memory", sb1.toString());
        tick += 1;
        final StringBuilder sb = new StringBuilder("TICK");
        sb.append(tick);
        System.out.println(sb.toString());
        currInput = 0;
        if (inputRate == 0) {
          currInputRate = Long.valueOf(poissonFile.nextLine());
        }


      }

      processedInput += 1;
      //while (System.nanoTime() - cTime < 1000000000*(1.0/inputRate)) {
        // adjust input rate
      //}
    }

    //countDownLatch.await();

    final long endTime = System.currentTimeMillis();
    metrics.setElapsedTime(endTime - currTime);
    mwo.close();
    wordGenerator.close();
    return metrics;

    /*
    final long partialCount = aggregationCounter.getNumPartialAggregation();
    final long finalCount = aggregationCounter.getNumFinalAggregation();
    //writer.writeLine(outputPath+"/result", "PARTIAL="+partialCount);
    //writer.writeLine(outputPath+"/result", "FINAL=" + finalCount);
    //writer.writeLine(outputPath+"/result", "ELAPSED_TIME="+(endTime-currTime));

    return new Result(partialCount, finalCount, processedInput, (endTime - currTime), timeMonitor);
  */
  }

  public static class Result {
    public final long partialCount;
    public final long finalCount;
    public final long elapsedTime;
    public final long processedInput;
    public final TimeMonitor timeMonitor;

    public Result(final long partialCount,
                  final long finalCount,
                  final long processedInput,
                  final long elapsedTime,
                  final TimeMonitor timeMonitor) {
      this.partialCount = partialCount;
      this.finalCount = finalCount;
      this.processedInput = processedInput;
      this.elapsedTime = elapsedTime;
      this.timeMonitor = timeMonitor;
    }
  }

  public static final class EvaluationHandler<I, O> implements TimeWindowOutputHandler<I, O> {

    private final String id;
    private final CountDownLatch countDownLatch;
    private final long totalTime;
    private final OutputWriter writer;
    private final String prefix;
    private final AtomicLong tickTime;
    private final Lock lock;
    private final Condition canProceed;

    private final ExecutorService executorService;

    private final Map<Long, AtomicInteger> outputCount;

    public EvaluationHandler(final String id, final CountDownLatch countDownLatch,
                             final long totalTime,
                             final OutputWriter writer,
                             final String prefix,
                             final AtomicLong tickTime,
                             final Lock lock,
                             final Map<Long, AtomicInteger> outputCount,
                             final Condition canProceed) {
      this.id = id;
      this.countDownLatch = countDownLatch;
      this.totalTime = totalTime;
      this.writer = writer;
      this.prefix = prefix;
      this.tickTime = tickTime;
      this.lock = lock;
      this.outputCount = outputCount;
      this.canProceed = canProceed;
      this.executorService = Executors.newSingleThreadExecutor();
    }

    @Override
    public void execute(final TimescaleWindowOutput<I> val) {
      final long endTime = System.currentTimeMillis();

      /*
      final long ttime = tickTime.get();
      final AtomicInteger count = outputCount.get(val.endTime);
      if (count.decrementAndGet() == 0) {
        lock.lock();
        canProceed.signal();
        lock.unlock();
      }
      */

      executorService.submit(new Runnable() {
        @Override
        public void run() {

          /*
          if (val.endTime <= totalTime) {
            if (countDownLatch != null) {
              countDownLatch.countDown();
            }
          }
          */

          try {
            final long elapsedTime = endTime - val.actualStartTime;
            final StringBuilder sb = new StringBuilder();
            sb.append(elapsedTime);
            sb.append("\t");
            sb.append(val.startTime);
            sb.append("\t");
            sb.append(val.endTime);
            sb.append("\t");
            sb.append(val.timescale);

            writer.writeLine(prefix + "_latency", sb.toString());

          } catch (final IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
          }
        }
      });
    }

    @Override
    public void prepare(final OutputEmitter<TimescaleWindowOutput<O>> outputEmitter) {

    }
  }

}
