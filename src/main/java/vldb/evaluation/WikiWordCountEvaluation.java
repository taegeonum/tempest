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


  public static void main(final String[] args) throws Exception {
    final int minWindowSize = 60;
    final int maxWindowSize = 600;
    final int minIntervalSize = 5;
    final int maxIntervalSize = 60;
    final int numThreads = 4;
    final double inputRate = 1000;
    final String wikiDataPath = "./dataset/big_wiki.txt";
    final String outputPath = "./log/" + System.currentTimeMillis();
    final Configuration commandLineConf = getCommandLineConf(args);
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder(commandLineConf);
    cb.bindNamedParameter(RandomSlidingWindowGenerator.MinWindowSize.class, minWindowSize+"");
    cb.bindNamedParameter(RandomSlidingWindowGenerator.MaxWindowSize.class, maxWindowSize+"");
    cb.bindNamedParameter(RandomSlidingWindowGenerator.MinIntervalSize.class, minIntervalSize+"");
    cb.bindNamedParameter(RandomSlidingWindowGenerator.MaxIntervalSize.class, maxIntervalSize+"");
    cb.bindNamedParameter(WikiWordGenerator.WikidataPath.class, wikiDataPath);
    final Injector injector = Tang.Factory.getTang().newInjector(cb.build());

    final Generator wordGenerator = injector.getInstance(WikiWordGenerator.class);
    final SlidingWindowGenerator swg = injector.getInstance(RandomSlidingWindowGenerator.class);

    final OutputWriter writer = injector.getInstance(LocalOutputWriter.class);

    // Print configuration
    writer.writeLine(outputPath, "!windowRange=[" + minWindowSize + "-" + maxWindowSize + "]");
    writer.writeLine(outputPath, "!intervalRange=[" + minIntervalSize + "-" + maxIntervalSize + "]");
    writer.writeLine(outputPath, "!Dataset=WikiWordGenerator");
    writer.writeLine(outputPath, "!InputRate=" + inputRate);

    final List<Integer> numWindows = Arrays.asList(20, 40, 60, 80, 100);
    for (final int numWindow : numWindows) {
      final List<Timescale> timescales = swg.generateSlidingWindows(numWindow);
      Collections.sort(timescales);
      final long totalTime = timescales.get(timescales.size()-1).windowSize * 3;

      final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
      jcb.bindImplementation(KeyExtractor.class, DefaultExtractor.class);
      jcb.bindNamedParameter(NumThreads.class, numThreads+"");

      final long currTime = System.currentTimeMillis();
      final List<Configuration> configurationList = new LinkedList<>();
      final List<String> operatorIds = new LinkedList<>();

      final String timescaleString = TimescaleParser.parseToString(timescales);
      writer.writeLine(outputPath, "@NumWindows=" + numWindow);
      writer.writeLine(outputPath, "@Windows="+timescaleString);
      // PAFAS-Greedy
      configurationList.add(StaticMWOConfiguration.CONF
          .set(StaticMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
          .set(StaticMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
          .set(StaticMWOConfiguration.SELECTION_ALGORITHM, GreedySelectionAlgorithm.class)
          .set(StaticMWOConfiguration.START_TIME, "0")
          .build());
      operatorIds.add("PAFAS");

      // On-the-fly operator
      configurationList.add(OntheflyMWOConfiguration.CONF
          .set(OntheflyMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
          .set(OntheflyMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
          .set(OntheflyMWOConfiguration.START_TIME, "0")
          .build());
      operatorIds.add("OntheFly");

      // TriOPs
      configurationList.add(TriOpsMWOConfiguration.CONF
          .set(TriOpsMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
          .set(TriOpsMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
          .set(TriOpsMWOConfiguration.START_TIME, "0")
          .build());
      operatorIds.add("TriOps");

      int i = 0;
      final List<AggregationCounter> aggregationCounters = new LinkedList<>();
      for (final Configuration conf : configurationList) {
        writer.writeLine(outputPath, "=================\nOperator=" + operatorIds.get(i));
        final Injector newInjector = Tang.Factory.getTang().newInjector(Configurations.merge(jcb.build(), conf));
        newInjector.bindVolatileInstance(TimeWindowOutputHandler.class, new LoggingHandler<>(operatorIds.get(i)));
        final TimescaleWindowOperator<String, Map<String, Long>> mwo = newInjector.getInstance(PafasMWO.class);
        final AggregationCounter aggregationCounter = newInjector.getInstance(AggregationCounter.class);
        aggregationCounters.add(aggregationCounter);
        i += 1;

        while (totalTime*1000 > (System.currentTimeMillis() - currTime)) {
          final String word = wordGenerator.nextString();
          final long cTime = System.nanoTime();
          if (word == null) {
            // End of input
            break;
          }
          mwo.execute(word);
          while (System.nanoTime() - cTime < 1000000000*(1.0/inputRate)) {
            // adjust input rate
          }
        }

        mwo.close();
        final long partialCount = aggregationCounter.getNumPartialAggregation();
        final long finalCount = aggregationCounter.getNumFinalAggregation();
        final String id = operatorIds.get(i);
        writer.writeLine(outputPath, "PARTIAL="+partialCount);
        writer.writeLine(outputPath, "FINAL=" + finalCount);
      }
    }
    writer.close();
  }
}
