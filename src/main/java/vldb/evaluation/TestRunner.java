package vldb.evaluation;

import org.apache.reef.tang.*;
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
import vldb.operator.window.timescale.pafas.DPSelectionAlgorithm;
import vldb.operator.window.timescale.pafas.GreedySelectionAlgorithm;
import vldb.operator.window.timescale.pafas.StaticMWOConfiguration;
import vldb.operator.window.timescale.parameter.NumThreads;
import vldb.operator.window.timescale.profiler.AggregationCounter;
import vldb.operator.window.timescale.triops.TriOpsMWOConfiguration;

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
    TriOps,
    OnTheFly,
    Naive
  }

  public static Result runTest(final List<Timescale> timescales,
                      final int numThreads,
                      final long numKey,
                      final OperatorType operatorType,
                      final double inputRate,
                      final long totalTime) throws Exception {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindImplementation(KeyExtractor.class, DefaultExtractor.class);
    jcb.bindNamedParameter(NumThreads.class, numThreads+"");
    //jcb.bindNamedParameter(WikiWordGenerator.WikidataPath.class, wikiDataPath);
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
    final Configuration conf;
    switch (operatorType) {
      case PAFAS:
        // PAFAS-Greedy
        conf = StaticMWOConfiguration.CONF
            .set(StaticMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
            .set(StaticMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
            .set(StaticMWOConfiguration.SELECTION_ALGORITHM, GreedySelectionAlgorithm.class)
            .set(StaticMWOConfiguration.START_TIME, "0")
            .build();
        break;
      case PAFAS_DP:
        conf = StaticMWOConfiguration.CONF
            .set(StaticMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
            .set(StaticMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
            .set(StaticMWOConfiguration.SELECTION_ALGORITHM, DPSelectionAlgorithm.class)
            .set(StaticMWOConfiguration.START_TIME, "0")
            .build();
        break;
      case OnTheFly:
        // On-the-fly operator
        conf = OntheflyMWOConfiguration.CONF
            .set(OntheflyMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
            .set(OntheflyMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
            .set(OntheflyMWOConfiguration.START_TIME, "0")
            .build();
        break;
      case TriOps:
        // TriOPs
        conf = TriOpsMWOConfiguration.CONF
            .set(TriOpsMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
            .set(TriOpsMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
            .set(TriOpsMWOConfiguration.START_TIME, "0")
            .build();
        break;
      case Naive:
        conf = NaiveMWOConfiguration.CONF
            .set(NaiveMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
            .set(NaiveMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
            .set(NaiveMWOConfiguration.START_TIME, "0")
            .build();
        break;
      default:
        throw new RuntimeException("Not implemented Naive");
    }
    //writer.writeLine(outputPath+"/result", "=================\nOperator=" + operatorType.name());
    final Injector newInjector = Tang.Factory.getTang().newInjector(Configurations.merge(jcb.build(), conf));
    final Generator wordGenerator = new ZipfianGenerator(numKey, 0.99);

    newInjector.bindVolatileInstance(TimeWindowOutputHandler.class,
        new EvaluationHandler<>(operatorType.name(), countDownLatch, totalTime));
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
    return new Result(partialCount, finalCount, (endTime - currTime));
  }

  public static class Result {
    public final long partialCount;
    public final long finalCount;
    public final long elapsedTime;

    public Result(final long partialCount,
                  final long finalCount,
                  final long elapsedTime) {
      this.partialCount = partialCount;
      this.finalCount = finalCount;
      this.elapsedTime = elapsedTime;
    }
  }


  public static final class EvaluationHandler<I, O> implements TimeWindowOutputHandler<I, O> {

    private final String id;
    private final CountDownLatch countDownLatch;
    private final long totalTime;

    public EvaluationHandler(final String id, final CountDownLatch countDownLatch, final long totalTime) {
      this.id = id;
      this.countDownLatch = countDownLatch;
      this.totalTime = totalTime;
    }

    @Override
    public void execute(final TimescaleWindowOutput<I> val) {
      if (val.endTime <= totalTime) {
        countDownLatch.countDown();
      }
      System.out.println(id + " ts: " + val.timescale +
          ", timespan: [" + val.startTime + ", " + val.endTime + ")");
    }

    @Override
    public void prepare(final OutputEmitter<TimescaleWindowOutput<O>> outputEmitter) {

    }
  }

}
