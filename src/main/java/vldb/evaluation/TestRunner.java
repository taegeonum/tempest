package vldb.evaluation;

import edu.snu.tempest.example.util.writer.OutputWriter;
import org.apache.reef.tang.*;
import vldb.evaluation.common.FileWordGenerator;
import vldb.evaluation.common.Generator;
import vldb.evaluation.common.ZipfianGenerator;
import vldb.evaluation.parameter.EndTime;
import vldb.example.DefaultExtractor;
import vldb.operator.OutputEmitter;
import vldb.operator.window.aggregator.impl.CountByKeyAggregator;
import vldb.operator.window.aggregator.impl.KeyExtractor;
import vldb.operator.window.timescale.*;
import vldb.operator.window.timescale.common.DefaultOutputLookupTableImpl;
import vldb.operator.window.timescale.common.TimescaleParser;
import vldb.operator.window.timescale.naive.NaiveMWOConfiguration;
import vldb.operator.window.timescale.onthefly.OntheflyMWOConfiguration;
import vldb.operator.window.timescale.pafas.*;
import vldb.operator.window.timescale.pafas.dynamic.*;
import vldb.operator.window.timescale.pafas.event.WindowTimeEvent;
import vldb.operator.window.timescale.parameter.NumThreads;
import vldb.operator.window.timescale.profiler.AggregationCounter;
import vldb.operator.window.timescale.triops.TriOpsMWOConfiguration;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * Created by taegeonum on 4/28/16.
 */
public final class TestRunner {
  //static final long totalTime = 1800;

  public enum OperatorType {
    DYNAMIC_DP,
    DYNAMIC_GREEDY,
    PAFAS,
    PAFAS_DP,
    PAFAS_SINGLE,
    PAFAS_SINGLE_ALL_STORE,
    PAFAS_SINGLE_NO_REFCNT,
    PAFAS_SINGLE_COUNT,
    PAFAS_INFINITE,
    PAFAS_SINGLE_GREEDY,
    PAFAS_SINGLE_GREEDY_COUNT,
    PAFAS_INFINITE_COUNT,
    PAFASI,
    PAFASI_DP,
    TriOps,
    OnTheFly,
    Naive
  }

  private static Configuration getOperatorConf(final OperatorType operatorType,
                                               final String timescaleString) {
    switch (operatorType) {
      case DYNAMIC_DP:
        return DynamicMWOConfiguration.CONF
          .set(DynamicMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
          .set(DynamicMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
          .set(DynamicMWOConfiguration.SELECTION_ALGORITHM, DynamicDPSelectionAlgorithm.class)
          .set(DynamicMWOConfiguration.OUTPUT_LOOKUP_TABLE, DynamicDPOutputLookupTableImpl.class)
          .set(DynamicMWOConfiguration.START_TIME, "0")
          .build();
      case DYNAMIC_GREEDY:
        return DynamicMWOConfiguration.CONF
            .set(DynamicMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
            .set(DynamicMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
            .set(DynamicMWOConfiguration.SELECTION_ALGORITHM, DynamicGreedySelectionAlgorithm.class)
            .set(DynamicMWOConfiguration.OUTPUT_LOOKUP_TABLE, DynamicGreedyOutputLookupTableImpl.class)
            .set(DynamicMWOConfiguration.START_TIME, "0")
            .build();
      case PAFAS_SINGLE:
        return StaticSingleMWOConfiguration.CONF
            .set(StaticSingleMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
            .set(StaticSingleMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
            .set(StaticSingleMWOConfiguration.SELECTION_ALGORITHM, DPSelectionAlgorithm.class)
            .set(StaticSingleMWOConfiguration.OUTPUT_LOOKUP_TABLE, DPOutputLookupTableImpl.class)
            .set(StaticSingleMWOConfiguration.START_TIME, "0")
            .build();
      case PAFAS_SINGLE_ALL_STORE:
        return StaticSingleAllStoreMWOConfiguration.CONF
            .set(StaticSingleAllStoreMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
            .set(StaticSingleAllStoreMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
            .set(StaticSingleAllStoreMWOConfiguration.SELECTION_ALGORITHM, DPSelectionAlgorithm.class)
            .set(StaticSingleAllStoreMWOConfiguration.OUTPUT_LOOKUP_TABLE, DPOutputLookupTableImpl.class)
            .set(StaticSingleAllStoreMWOConfiguration.START_TIME, "0")
            .build();
      case PAFAS_SINGLE_NO_REFCNT:
        return StaticSingleNoRefCntMWOConfiguration.CONF
            .set(StaticSingleNoRefCntMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
            .set(StaticSingleNoRefCntMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
            .set(StaticSingleNoRefCntMWOConfiguration.SELECTION_ALGORITHM, DPSelectionAlgorithm.class)
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
      default:
        throw new RuntimeException("Not implemented Naive");
    }
  }

  public static Result runFileWordTest(final List<Timescale> timescales,
                                       final int numThreads,
                                       final String filePath,
                                       final OperatorType operatorType,
                                       final double inputRate,
                                       final long totalTime,
                                       final OutputWriter writer,
                                       final String prefix) throws Exception {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindImplementation(KeyExtractor.class, DefaultExtractor.class);
    jcb.bindNamedParameter(NumThreads.class, numThreads+"");
    jcb.bindNamedParameter(FileWordGenerator.FileDataPath.class, filePath);
    jcb.bindNamedParameter(EndTime.class, totalTime+"");

    Collections.sort(timescales);
    int numOutputs = 0;
    for (final Timescale ts : timescales) {
      numOutputs += (totalTime / ts.intervalSize);
    }
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

      if (processedInput % ((long)inputRate) == 0) {
        mwo.execute(new WindowTimeEvent(tick));
        tick += 1;
      }


      processedInput += 1;
      //while (System.nanoTime() - cTime < 1000000000*(1.0/inputRate)) {
        // adjust input rate
      //}
    }
    final long endTime = System.currentTimeMillis();
    countDownLatch.await();
    mwo.close();
    wordGenerator.close();
    final long partialCount = aggregationCounter.getNumPartialAggregation();
    final long finalCount = aggregationCounter.getNumFinalAggregation();
    //writer.writeLine(outputPath+"/result", "PARTIAL="+partialCount);
    //writer.writeLine(outputPath+"/result", "FINAL=" + finalCount);
    //writer.writeLine(outputPath+"/result", "ELAPSED_TIME="+(endTime-currTime));
    return new Result(partialCount, finalCount, processedInput, (endTime - currTime), timeMonitor);
  }


  public static Result runTest(final List<Timescale> timescales,
                               final int numThreads,
                               final long numKey,
                               final OperatorType operatorType,
                               final double inputRate,
                               final long totalTime,
                               final OutputWriter writer,
                               final String prefix) throws Exception {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindImplementation(KeyExtractor.class, DefaultExtractor.class);
    jcb.bindNamedParameter(NumThreads.class, numThreads+"");
    //jcb.bindNamedParameter(FileWordGenerator.FileDataPath.class, wikiDataPath);
    jcb.bindNamedParameter(EndTime.class, totalTime+"");

    Collections.sort(timescales);
    int numOutputs = 0;
    for (final Timescale ts : timescales) {
      numOutputs += (totalTime / ts.intervalSize);
    }
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
    final Generator wordGenerator = new ZipfianGenerator(numKey, 0.99);

    newInjector.bindVolatileInstance(TimeWindowOutputHandler.class,
        new EvaluationHandler<>(operatorType.name(), countDownLatch, totalTime, writer, prefix));
    final TimescaleWindowOperator<String, Map<String, Long>> mwo = newInjector.getInstance(TimescaleWindowOperator.class);
    final AggregationCounter aggregationCounter = newInjector.getInstance(AggregationCounter.class);
    i += 1;

    final long currTime = System.currentTimeMillis();
    long processedInput = 0;
    while (processedInput < inputRate * totalTime) {
      //System.out.println("totalTime: " + (totalTime*1000) + ", elapsed: " + (System.currentTimeMillis() - currTime));
      final String word = wordGenerator.nextString();
      final long cTime = System.nanoTime();
      //System.out.println("word: " + word);
      if (word == null) {
        // End of input
        break;
      }
      mwo.execute(word);
      processedInput += 1;
      while (System.nanoTime() - cTime < 1000000000*(1.0/inputRate)) {
        // adjust input rate
      }
    }
    final long endTime = System.currentTimeMillis();
    countDownLatch.await();
    mwo.close();
    wordGenerator.close();
    final long partialCount = aggregationCounter.getNumPartialAggregation();
    final long finalCount = aggregationCounter.getNumFinalAggregation();
    //writer.writeLine(outputPath+"/result", "PARTIAL="+partialCount);
    //writer.writeLine(outputPath+"/result", "FINAL=" + finalCount);
    //writer.writeLine(outputPath+"/result", "ELAPSED_TIME="+(endTime-currTime));
    return new Result(partialCount, finalCount, 0, (endTime - currTime), null);
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
      if (val.endTime <= totalTime) {
        countDownLatch.countDown();
      }
      try {
        writer.writeLine(prefix + "/" + val.timescale, System.currentTimeMillis()+"\t" + val.startTime + "\t" + val.endTime);
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
      System.out.println(id + " ts: " + val.timescale +
          ", timespan: [" + val.startTime + ", " + val.endTime + ")");
    }

    @Override
    public void prepare(final OutputEmitter<TimescaleWindowOutput<O>> outputEmitter) {

    }
  }

}
