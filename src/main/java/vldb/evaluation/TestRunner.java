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
import vldb.operator.window.timescale.naive.NaiveMWOConfiguration;
import vldb.operator.window.timescale.onthefly.OntheflyMWOConfiguration;
import vldb.operator.window.timescale.pafas.DPOutputLookupTableImpl;
import vldb.operator.window.timescale.pafas.EagerMWOConfiguration;
import vldb.operator.window.timescale.pafas.PeriodCalculator;
import vldb.operator.window.timescale.pafas.StaticSingleMWOConfiguration;
import vldb.operator.window.timescale.pafas.active.*;
import vldb.operator.window.timescale.pafas.dynamic.DynamicDPOutputLookupTableImpl;
import vldb.operator.window.timescale.pafas.dynamic.DynamicOptimizedDependencyGraphImpl;
import vldb.operator.window.timescale.pafas.event.WindowTimeEvent;
import vldb.operator.window.timescale.parameter.NumThreads;
import vldb.operator.window.timescale.parameter.ReusingRatio;
import vldb.operator.window.timescale.parameter.SharedFinalNum;
import vldb.operator.window.timescale.parameter.WindowGap;
import vldb.operator.window.timescale.profiler.AggregationCounter;
import vldb.operator.window.timescale.triops.TriOpsMWOConfiguration;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;

/**
 * Created by taegeonum on 4/28/16.
 */
public final class TestRunner {
  //static final long totalTime = 1800;

  public enum OperatorType {
    FastSt, // fast static
    FastDy, // fast dynamic
    FastSm, // fast static memory
    FastEg, // fast eager agg
    FastPruning, // fast pruning after DP
    FastRb, // fast rebuild
    FastRb2, // fast rebuild with weight
    FastRbNum, // fast rebuild with num limited num
    NoOverlap, // no overlap
    FastOverlap, // rm overlapping windows
    OTFSta,
    OTFDyn,
    TriOps,
    Naivee,
    Cuttyy,
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
            .set(StaticSingleMWOConfiguration.DEPENDENCY_GRAPH, WindowPruningDependencyGraphImpl.class)
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

    newInjector.bindVolatileInstance(TimeWindowOutputHandler.class,
        new EvaluationHandler<>(operatorType.name(), null, totalTime, writer, prefix));
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
                                       final int sharedFinalNum) throws Exception {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindImplementation(KeyExtractor.class, DefaultExtractor.class);
    jcb.bindNamedParameter(NumThreads.class, numThreads+"");
    jcb.bindNamedParameter(FileWordGenerator.FileDataPath.class, filePath);
    jcb.bindNamedParameter(EndTime.class, totalTime+"");
    jcb.bindNamedParameter(ReusingRatio.class, reusingRatio+"");
    jcb.bindNamedParameter(WindowGap.class, windowGap+"");
    jcb.bindNamedParameter(SharedFinalNum.class, sharedFinalNum+"");

    Collections.sort(timescales);
    int numOutputs = 0;
    for (final Timescale ts : timescales) {
      numOutputs += (totalTime / ts.intervalSize);
    }
    writer.writeLine(prefix + "_num_outputs", numOutputs+"");
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

    newInjector.bindVolatileInstance(TimeWindowOutputHandler.class,
        new EvaluationHandler<>(operatorType.name(), countDownLatch, totalTime, writer, prefix));
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
        final StringBuilder sb1 = new StringBuilder();
        sb1.append(System.currentTimeMillis()); sb1.append("\t"); sb1.append(Profiler.getMemoryUsage()); sb1.append("\t");
        sb1.append(metrics.storedPartial);
        sb1.append("\t");
        sb1.append(metrics.storedFinal);
        sb1.append("\t");
        sb1.append(metrics.storedPartial + metrics.storedFinal);
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

    public EvaluationHandler(final String id, final CountDownLatch countDownLatch, final long totalTime,
                             final OutputWriter writer,
                             final String prefix) {
      this.id = id;
      this.countDownLatch = countDownLatch;
      this.totalTime = totalTime;
      this.writer = writer;
      this.prefix = prefix;
    }

    @Override
    public void execute(final TimescaleWindowOutput<I> val) {
      /*if (val.endTime <= totalTime) {
        if (countDownLatch != null) {
          countDownLatch.countDown();
        }
      }*/
      /*
      try {
        writer.writeLine(prefix + "/" + val.timescale, System.currentTimeMillis()+"\t" + val.startTime + "\t" + val.endTime);
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
      */
      /*
      if (val.endTime % 100 == 0) {
        final StringBuilder sb = new StringBuilder();
        sb.append(id);
        sb.append(" ts: ");
        sb.append(val.timescale);
        sb.append(", timespan: [");
        sb.append(val.startTime);
        sb.append(", ");
        sb.append(val.endTime);
        sb.append(")");
        System.out.println(sb.toString());
      }
      */
    }

    @Override
    public void prepare(final OutputEmitter<TimescaleWindowOutput<O>> outputEmitter) {

    }
  }

}
