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
import vldb.operator.window.timescale.TimeWindowOutputHandler;
import vldb.operator.window.timescale.Timescale;
import vldb.operator.window.timescale.TimescaleWindowOperator;
import vldb.operator.window.timescale.TimescaleWindowOutput;
import vldb.operator.window.timescale.common.TimescaleParser;
import vldb.operator.window.timescale.naive.NaiveMWOConfiguration;
import vldb.operator.window.timescale.onthefly.OntheflyMWOConfiguration;
import vldb.operator.window.timescale.pafas.*;
import vldb.operator.window.timescale.pafas.infinite.InfiniteCountMWOConfiguration;
import vldb.operator.window.timescale.pafas.infinite.InfiniteMWOConfiguration;
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
    PAFAS,
    PAFAS_DP,
    PAFAS_SINGLE,
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
      case PAFAS_SINGLE_COUNT:
        return StaticSingleCountMWOConfiguration.CONF
            .set(StaticSingleCountMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
            .set(StaticSingleCountMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
            .set(StaticSingleCountMWOConfiguration.SELECTION_ALGORITHM, DPSelectionAlgorithm.class)
            .set(StaticSingleCountMWOConfiguration.START_TIME, "0")
            .build();
      case PAFAS_INFINITE_COUNT:
        return InfiniteCountMWOConfiguration.CONF
            .set(InfiniteCountMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
            .set(InfiniteCountMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
            .set(InfiniteCountMWOConfiguration.SELECTION_ALGORITHM, DPSelectionAlgorithm.class)
            .set(InfiniteCountMWOConfiguration.START_TIME, "0")
            .build();
      case PAFAS_SINGLE_GREEDY_COUNT:
        return StaticSingleCountMWOConfiguration.CONF
            .set(StaticSingleCountMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
            .set(StaticSingleCountMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
            .set(StaticSingleCountMWOConfiguration.SELECTION_ALGORITHM, GreedySelectionAlgorithm.class)
            .set(StaticSingleCountMWOConfiguration.START_TIME, "0")
            .build();
      case PAFAS_SINGLE:
        return StaticSingleMWOConfiguration.CONF
            .set(StaticSingleMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
            .set(StaticSingleMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
            .set(StaticSingleMWOConfiguration.SELECTION_ALGORITHM, DPSelectionAlgorithm.class)
            .set(StaticSingleMWOConfiguration.START_TIME, "0")
            .build();
      case PAFAS_SINGLE_GREEDY:
        return StaticSingleMWOConfiguration.CONF
            .set(StaticSingleMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
            .set(StaticSingleMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
            .set(StaticSingleMWOConfiguration.SELECTION_ALGORITHM, GreedySelectionAlgorithm.class)
            .set(StaticSingleMWOConfiguration.START_TIME, "0")
            .build();
      case PAFAS_INFINITE:
        return InfiniteMWOConfiguration.CONF
            .set(InfiniteMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
            .set(InfiniteMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
            .set(InfiniteMWOConfiguration.SELECTION_ALGORITHM, DPSelectionAlgorithm.class)
            .set(InfiniteMWOConfiguration.START_TIME, "0")
            .build();
      case PAFAS:
        // PAFAS-Greedy
        return StaticMWOConfiguration.CONF
            .set(StaticMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
            .set(StaticMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
            .set(StaticMWOConfiguration.SELECTION_ALGORITHM, GreedySelectionAlgorithm.class)
            .set(StaticMWOConfiguration.START_TIME, "0")
            .build();
      case PAFASI:
        // PAFAS Incremental-Greedy
        return IncrementMWOConfiguration.CONF
            .set(IncrementMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
            .set(IncrementMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
            .set(IncrementMWOConfiguration.SELECTION_ALGORITHM, GreedySelectionAlgorithm.class)
            .set(IncrementMWOConfiguration.START_TIME, "0")
            .build();
      case PAFASI_DP:
        // PAFAS Incremental-DP
        return IncrementMWOConfiguration.CONF
            .set(IncrementMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
            .set(IncrementMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
            .set(IncrementMWOConfiguration.SELECTION_ALGORITHM, DPSelectionAlgorithm.class)
            .set(IncrementMWOConfiguration.START_TIME, "0")
            .build();
      case PAFAS_DP:
        return StaticMWOConfiguration.CONF
            .set(StaticMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
            .set(StaticMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
            .set(StaticMWOConfiguration.SELECTION_ALGORITHM, DPSelectionAlgorithm.class)
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
    final TimescaleWindowOperator<String, Map<String, Long>> mwo = newInjector.getInstance(TimescaleWindowOperator.class);
    final AggregationCounter aggregationCounter = newInjector.getInstance(AggregationCounter.class);
    final PeriodCalculator periodCalculator = newInjector.getInstance(PeriodCalculator.class);
    final long period = periodCalculator.getPeriod();
    i += 1;

    final long currTime = System.currentTimeMillis();
    long processedInput = 0;
    //Thread.sleep(period);
    // FOR TIME
    //while (processedInput < inputRate * (totalTime)) {
    // FOR COUNT
    while (processedInput < 100 * (totalTime)) {
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
    return new Result(partialCount, finalCount, processedInput, (endTime - currTime));
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
    return new Result(partialCount, finalCount, 0, (endTime - currTime));
  }

  public static class Result {
    public final long partialCount;
    public final long finalCount;
    public final long elapsedTime;
    public final long processedInput;

    public Result(final long partialCount,
                  final long finalCount,
                  final long processedInput,
                  final long elapsedTime) {
      this.partialCount = partialCount;
      this.finalCount = finalCount;
      this.processedInput = processedInput;
      this.elapsedTime = elapsedTime;
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
