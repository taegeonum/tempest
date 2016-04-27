package vldb.evaluation;

import edu.snu.tempest.example.util.writer.LocalOutputWriter;
import edu.snu.tempest.example.util.writer.OutputWriter;
import org.apache.reef.tang.*;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.formats.CommandLine;
import vldb.evaluation.common.Generator;
import vldb.evaluation.common.WikiWordGenerator;
import vldb.evaluation.util.LoggingHandler;
import vldb.evaluation.util.RandomSlidingWindowGenerator;
import vldb.evaluation.util.SlidingWindowGenerator;
import vldb.example.DefaultExtractor;
import vldb.operator.window.aggregator.impl.CountByKeyAggregator;
import vldb.operator.window.aggregator.impl.KeyExtractor;
import vldb.operator.window.timescale.TimeWindowOutputHandler;
import vldb.operator.window.timescale.Timescale;
import vldb.operator.window.timescale.TimescaleWindowOperator;
import vldb.operator.window.timescale.common.TimescaleParser;
import vldb.operator.window.timescale.onthefly.OntheflyMWOConfiguration;
import vldb.operator.window.timescale.pafas.GreedySelectionAlgorithm;
import vldb.operator.window.timescale.pafas.PafasMWO;
import vldb.operator.window.timescale.pafas.StaticMWOConfiguration;
import vldb.operator.window.timescale.parameter.NumThreads;
import vldb.operator.window.timescale.profiler.AggregationCounter;
import vldb.operator.window.timescale.triops.TriOpsMWOConfiguration;

import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;

/**
 * Created by taegeonum on 4/25/16.
 */
public final class WikiWordCountEvaluation {
  private static final Logger LOG = Logger.getLogger(WikiWordCountEvaluation.class.getName());

  /**
   * Parameters.
   */
  static final int minWindowSize = 60;
  static final int maxWindowSize = 600;
  static final int minIntervalSize = 5;
  static final int maxIntervalSize = 50;
  static final int numThreads = 16;
  static final double inputRate = 1000;
  static final String wikiDataPath = "./dataset/big_wiki.txt";
  static final String outputPath = "./log/" + System.currentTimeMillis();


  /**
   * Parse command line arguments.
   */
  private static Configuration getCommandLineConf(String[] args) throws BindException, IOException {
    final Tang tang = Tang.Factory.getTang();
    final JavaConfigurationBuilder cb = tang.newConfigurationBuilder();

    final CommandLine cl = new CommandLine(cb)
        //.registerShortNameOfClass(TestName.class)
        //.registerShortNameOfClass(LogDir.class)
        //.registerShortNameOfClass(WikiWordGenerator.WikidataPath.class)
        .processCommandLine(args);

    return cl.getBuilder().build();
  }

  public static void runTest(final List<Timescale> timescales,
                             final OutputWriter writer,
                             final double inputRate) throws Exception {
    final long totalTime = 1200;

    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindImplementation(KeyExtractor.class, DefaultExtractor.class);
    jcb.bindNamedParameter(NumThreads.class, numThreads+"");
    jcb.bindNamedParameter(WikiWordGenerator.WikidataPath.class, wikiDataPath);


    final List<String> operatorIds = new LinkedList<>();

    final String timescaleString = TimescaleParser.parseToString(timescales);
    writer.writeLine(outputPath, "-------------------------------------");
    writer.writeLine(outputPath, "@NumWindows=" + timescales.size());
    writer.writeLine(outputPath, "@Windows="+timescaleString);
    writer.writeLine(outputPath, "-------------------------------------");

    operatorIds.add("PAFAS");
    operatorIds.add("OntheFly");
    operatorIds.add("TriOps");

    int i = 0;
    final List<AggregationCounter> aggregationCounters = new LinkedList<>();
    for (final String id : operatorIds) {
      final long currTime = System.currentTimeMillis();
      final Configuration conf;
      switch (id) {
        case "PAFAS":
          // PAFAS-Greedy
          conf = StaticMWOConfiguration.CONF
              .set(StaticMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
              .set(StaticMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
              .set(StaticMWOConfiguration.SELECTION_ALGORITHM, GreedySelectionAlgorithm.class)
              .set(StaticMWOConfiguration.START_TIME, "0")
              .build();
          break;
        case "OntheFly":
          // On-the-fly operator
          conf = OntheflyMWOConfiguration.CONF
              .set(OntheflyMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
              .set(OntheflyMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
              .set(OntheflyMWOConfiguration.START_TIME, "0")
              .build();
          break;
        case "TriOps":
          // TriOPs
          conf = TriOpsMWOConfiguration.CONF
              .set(TriOpsMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
              .set(TriOpsMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
              .set(TriOpsMWOConfiguration.START_TIME, "0")
              .build();
          break;
        default:
          conf = null;
      }
      writer.writeLine(outputPath, "=================\nOperator=" + operatorIds.get(i));
      final Injector newInjector = Tang.Factory.getTang().newInjector(Configurations.merge(jcb.build(), conf));
      final Generator wordGenerator = newInjector.getInstance(WikiWordGenerator.class);

      newInjector.bindVolatileInstance(TimeWindowOutputHandler.class, new LoggingHandler<>(operatorIds.get(i)));
      final TimescaleWindowOperator<String, Map<String, Long>> mwo = newInjector.getInstance(PafasMWO.class);
      final AggregationCounter aggregationCounter = newInjector.getInstance(AggregationCounter.class);
      aggregationCounters.add(aggregationCounter);
      i += 1;

      while (totalTime*1000 > (System.currentTimeMillis() - currTime)) {
        //System.out.println("totalTime: " + (totalTime*1000) + ", elapsed: " + (System.currentTimeMillis() - currTime));
        final String word = wordGenerator.nextString();
        final long cTime = System.nanoTime();
        //System.out.println("word: " + word);
        if (word == null) {
          // End of input
          break;
        }
        mwo.execute(word);
        while (System.nanoTime() - cTime < 1000000000*(1.0/inputRate)) {
          // adjust input rate
        }
      }
      final long endTime = System.currentTimeMillis();
      mwo.close();
      final long partialCount = aggregationCounter.getNumPartialAggregation();
      final long finalCount = aggregationCounter.getNumFinalAggregation();
      writer.writeLine(outputPath, "PARTIAL="+partialCount);
      writer.writeLine(outputPath, "FINAL=" + finalCount);
      writer.writeLine(outputPath, "ELAPSED_TIME="+(endTime-currTime));
    }
  }

  public static void main(final String[] args) throws Exception {
    final Configuration commandLineConf = getCommandLineConf(args);

    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder(commandLineConf);
    cb.bindNamedParameter(RandomSlidingWindowGenerator.MinWindowSize.class, minWindowSize + "");
    cb.bindNamedParameter(RandomSlidingWindowGenerator.MaxWindowSize.class, maxWindowSize + "");
    cb.bindNamedParameter(RandomSlidingWindowGenerator.MinIntervalSize.class, minIntervalSize + "");
    cb.bindNamedParameter(RandomSlidingWindowGenerator.MaxIntervalSize.class, maxIntervalSize + "");
    final Injector injector = Tang.Factory.getTang().newInjector(cb.build());

    SlidingWindowGenerator swg = injector.getInstance(RandomSlidingWindowGenerator.class);

    final OutputWriter writer = injector.getInstance(LocalOutputWriter.class);

    /*
    // Print configuration
    writer.writeLine(outputPath, "============ [Experiment1]: Change the number of windows=============");
    writer.writeLine(outputPath, "!windowRange=[" + minWindowSize + "-" + maxWindowSize + "]");
    writer.writeLine(outputPath, "!intervalRange=[" + minIntervalSize + "-" + maxIntervalSize + "]");
    writer.writeLine(outputPath, "!Dataset=WikiWordGenerator");
    writer.writeLine(outputPath, "!InputRate=" + inputRate);

    // Experiment1: Change the number of windows
    final List<Integer> numWindows = Arrays.asList(10, 20, 30, 40, 50);
    for (final int numWindow : numWindows) {
      final List<Timescale> timescales = swg.generateSlidingWindows(numWindow);
      Collections.sort(timescales);
      runTest(timescales, writer);
    }
    writer.writeLine(outputPath, "============ [Experiment1]: END OF EXPERIMENT =============");

    // Experiment2: Change the window size
    writer.writeLine(outputPath, "============ [Experiment2]: Change the window size =============");
    final JavaConfigurationBuilder jcb1 = Tang.Factory.getTang().newConfigurationBuilder();
    jcb1.bindNamedParameter(RandomSlidingWindowGenerator.MinWindowSize.class, (minWindowSize/5)+"");
    jcb1.bindNamedParameter(RandomSlidingWindowGenerator.MaxWindowSize.class, (maxWindowSize/5)+"");
    jcb1.bindNamedParameter(RandomSlidingWindowGenerator.MinIntervalSize.class, minIntervalSize+"");
    jcb1.bindNamedParameter(RandomSlidingWindowGenerator.MaxIntervalSize.class, maxIntervalSize+"");
    final Injector injector1 = Tang.Factory.getTang().newInjector(jcb1.build());
    swg = injector1.getInstance(RandomSlidingWindowGenerator.class);

    final List<Timescale> exp2Timescale = swg.generateSlidingWindows(20);
    final List<Integer> multiples = Arrays.asList(1, 2, 3, 4, 5);
    for (final int multiple : multiples) {
      final List<Timescale> ts = new LinkedList<>();
      for (final Timescale timescale : exp2Timescale) {
        ts.add(new Timescale(timescale.windowSize*multiple, timescale.intervalSize));
      }
      runTest(ts, writer);
    }
    writer.writeLine(outputPath, "============ [Experiment2]: END OF EXPERIMENT =============");

    // Experiment3: Change the interval size
    // Pick 40 number of windows
    writer.writeLine(outputPath, "============ [Experiment3]: Change the interval size =============");
    writer.writeLine(outputPath, "!windowRange=[" + minWindowSize + "-" + maxWindowSize + "]");
    writer.writeLine(outputPath, "!intervalRange=[" + 5 + "-" + 10 + "]");
    writer.writeLine(outputPath, "!Dataset=WikiWordGenerator");
    writer.writeLine(outputPath, "!InputRate=" + inputRate);

    final JavaConfigurationBuilder jcb2 = Tang.Factory.getTang().newConfigurationBuilder();
    jcb2.bindNamedParameter(RandomSlidingWindowGenerator.MinWindowSize.class, minWindowSize + "");
    jcb2.bindNamedParameter(RandomSlidingWindowGenerator.MaxWindowSize.class, maxWindowSize + "");
    jcb2.bindNamedParameter(RandomSlidingWindowGenerator.MinIntervalSize.class, 5 + "");
    jcb2.bindNamedParameter(RandomSlidingWindowGenerator.MaxIntervalSize.class, 10 + "");
    final Injector injector2 = Tang.Factory.getTang().newInjector(jcb2.build());
    swg = injector2.getInstance(RandomSlidingWindowGenerator.class);

    final List<Timescale> exp3Timescale = swg.generateSlidingWindows(20);
    final List<Integer> intervalMultiples = Arrays.asList(1, 2, 3, 4, 5);
    for (final int multiple : intervalMultiples) {
      final List<Timescale> ts = new LinkedList<>();
      for (final Timescale timescale : exp3Timescale) {
        ts.add(new Timescale(timescale.windowSize, timescale.intervalSize * multiple));
      }
      runTest(ts, writer, inputRate);
    }
    writer.writeLine(outputPath, "============ [Experiment3]: END OF EXPERIMENT =============");
    */

    writer.writeLine(outputPath, "============ [Experiment3]: Change the interval size =============");
    final List<Timescale> interval6 = TimescaleParser.parseFromString("(490,60)(560,60)(205,30)(435,60)(595,60)(240,30)(80,30)(145,30)(305,30)(85,30)(315,60)(470,30)(220,60)(120,30)(125,60)(415,60)(380,30)(130,60)(290,30)(520,60)");
    Collections.sort(interval6);
    runTest(interval6, writer, inputRate);

    final List<Timescale> interval7 = TimescaleParser.parseFromString("(490,70)(560,70)(205,35)(435,70)(595,70)(240,35)(80,35)(145,35)(305,35)(85,35)(315,70)(470,35)(220,70)(120,35)(125,70)(415,70)(380,35)(130,70)(290,35)(520,70)");
    Collections.sort(interval7);
    runTest(interval7, writer, inputRate);
    writer.writeLine(outputPath, "============ [Experiment3]: END OF EXPERIMENT =============");

    // Experiment4: Change the input rate
    // Pick 20 number of windows
    writer.writeLine(outputPath, "============ [Experiment4]: Change the input rate =============");
    writer.writeLine(outputPath, "!windowRange=[" + minWindowSize + "-" + maxWindowSize + "]");
    writer.writeLine(outputPath, "!intervalRange=[" + minIntervalSize + "-" + maxIntervalSize + "]");
    writer.writeLine(outputPath, "!Dataset=WikiWordGenerator");

    final JavaConfigurationBuilder jcb3 = Tang.Factory.getTang().newConfigurationBuilder();
    jcb3.bindNamedParameter(RandomSlidingWindowGenerator.MinWindowSize.class, minWindowSize + "");
    jcb3.bindNamedParameter(RandomSlidingWindowGenerator.MaxWindowSize.class, maxWindowSize + "");
    jcb3.bindNamedParameter(RandomSlidingWindowGenerator.MinIntervalSize.class, minIntervalSize+"");
    jcb3.bindNamedParameter(RandomSlidingWindowGenerator.MaxIntervalSize.class, maxIntervalSize+"");
    final Injector injector3 = Tang.Factory.getTang().newInjector(jcb3.build());
    swg = injector3.getInstance(RandomSlidingWindowGenerator.class);

    final List<Timescale> exp4Timescale = swg.generateSlidingWindows(20);
    Collections.sort(exp4Timescale);
    final List<Double> inputRates = Arrays.asList(500.0, 1000.0, 1500.0, 2000.0, 2500.0);
    for (final double inputRate : inputRates) {
      writer.writeLine(outputPath, "!InputRate=" + inputRate);
      runTest(exp4Timescale, writer, inputRate);
    }
    writer.writeLine(outputPath, "============ [Experiment4]: END OF EXPERIMENT =============");

    // End of experiments
    Thread.sleep(20000);
    writer.close();
  }
}
