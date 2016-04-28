package vldb.evaluation;

import edu.snu.tempest.example.util.writer.OutputWriter;
import org.apache.reef.tang.*;
import vldb.evaluation.common.Generator;
import vldb.evaluation.common.WikiWordGenerator;
import vldb.evaluation.util.LoggingHandler;
import vldb.evaluation.util.Profiler;
import vldb.example.DefaultExtractor;
import vldb.operator.window.aggregator.impl.CountByKeyAggregator;
import vldb.operator.window.aggregator.impl.KeyExtractor;
import vldb.operator.window.timescale.TimeWindowOutputHandler;
import vldb.operator.window.timescale.Timescale;
import vldb.operator.window.timescale.TimescaleWindowOperator;
import vldb.operator.window.timescale.common.TimescaleParser;
import vldb.operator.window.timescale.onthefly.OntheflyMWOConfiguration;
import vldb.operator.window.timescale.pafas.DPSelectionAlgorithm;
import vldb.operator.window.timescale.pafas.GreedySelectionAlgorithm;
import vldb.operator.window.timescale.pafas.PafasMWO;
import vldb.operator.window.timescale.pafas.StaticMWOConfiguration;
import vldb.operator.window.timescale.parameter.NumThreads;
import vldb.operator.window.timescale.profiler.AggregationCounter;
import vldb.operator.window.timescale.triops.TriOpsMWOConfiguration;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by taegeonum on 4/28/16.
 */
public final class TestRunner {
  static final long totalTime = 10;

  public enum OperatorType {
    PAFAS,
    PAFAS_DP,
    TriOps,
    OnTheFly,
    Naive
  }

  public static Result runTest(final List<Timescale> timescales,
                      final OutputWriter writer,
                      final String outputPath,
                      final String testName,
                      final int numThreads,
                      final String wikiDataPath,
                      final OperatorType operatorType,
                      final double inputRate) throws Exception {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindImplementation(KeyExtractor.class, DefaultExtractor.class);
    jcb.bindNamedParameter(NumThreads.class, numThreads+"");
    jcb.bindNamedParameter(WikiWordGenerator.WikidataPath.class, wikiDataPath);

    Collections.sort(timescales);
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
        // TODO
        throw new RuntimeException("Not implemented Naive");
      default:
        throw new RuntimeException("Not implemented Naive");
    }
    //writer.writeLine(outputPath+"/result", "=================\nOperator=" + operatorType.name());
    final Injector newInjector = Tang.Factory.getTang().newInjector(Configurations.merge(jcb.build(), conf));
    final Generator wordGenerator = newInjector.getInstance(WikiWordGenerator.class);

    newInjector.bindVolatileInstance(TimeWindowOutputHandler.class, new LoggingHandler<>(operatorType.name()));
    final TimescaleWindowOperator<String, Map<String, Long>> mwo = newInjector.getInstance(PafasMWO.class);
    final AggregationCounter aggregationCounter = newInjector.getInstance(AggregationCounter.class);
    i += 1;

    // Profiler
    final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
    executorService.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        try {
          writer.writeLine(outputPath+"/" + testName + "-" + operatorType.name() + "-memory", System.currentTimeMillis() + "\t" + Profiler.getMemoryUsage());
          writer.writeLine(outputPath+"/" + testName + "-" + operatorType.name() + "-computation", System.currentTimeMillis() + "\t" + aggregationCounter.getNumPartialAggregation() +"\t" + aggregationCounter.getNumFinalAggregation());
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }, 1, 1, TimeUnit.SECONDS);


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
    mwo.close();
    executorService.shutdownNow();
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
}
