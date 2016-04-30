package vldb.evaluation;

import edu.snu.tempest.example.util.writer.LocalOutputWriter;
import edu.snu.tempest.example.util.writer.OutputWriter;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.formats.CommandLine;
import vldb.evaluation.util.EvaluationUtils;
import vldb.evaluation.util.ProcessHelper;
import vldb.evaluation.util.StreamGobbler;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

/**
 * Created by taegeonum on 4/25/16.
 */
public final class EvaluationLauncher3 {
  private static final Logger LOG = Logger.getLogger(EvaluationLauncher3.class.getName());

  /**
   * Parameters.
   */
  static final int minWindowSize = 50;
  static final int maxWindowSize = 500;
  static final int minIntervalSize = 1;
  static final int maxIntervalSize = 10;
  static final int numThreads = 16;
  static final double inputRate = 10000;
  static final String wikiDataPath = "./dataset/big_wiki.txt";
  static final String outputPath = "./log/0429-wordcount/interval-size";

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

    // Experiment1: Change the number of windows
    final List<TestRunner.OperatorType> operatorTypes = Arrays.asList(
        TestRunner.OperatorType.PAFAS,
        TestRunner.OperatorType.PAFAS_DP,
        TestRunner.OperatorType.TriOps,
        TestRunner.OperatorType.OnTheFly,
        TestRunner.OperatorType.Naive
    );

    final List<String> timescalesList = new LinkedList<>();
    timescalesList.add("(104,9)(296,9)(301,7)(245,7)(87,6)(379,3)(288,8)(476,4)(414,6)(260,5)(256,1)(69,3)(451,1)(453,3)(234,6)(103,2)(497,10)(402,9)(180,10)(113,6)(461,1)(440,10)(240,2)(401,1)(410,3)(194,10)(478,5)(447,4)(129,4)(257,4)");
    timescalesList.add("(104,18)(296,18)(301,14)(245,14)(87,12)(379,6)(288,16)(476,8)(414,12)(260,10)(256,2)(69,6)(451,2)(453,6)(234,12)(103,4)(497,20)(402,18)(180,20)(113,12)(461,2)(440,20)(240,4)(401,2)(410,6)(194,20)(478,10)(447,8)(129,8)(257,8)");
    timescalesList.add("(104,27)(296,27)(301,21)(245,21)(87,18)(379,9)(288,24)(476,12)(414,18)(260,15)(256,3)(69,9)(451,3)(453,9)(234,18)(103,6)(497,30)(402,27)(180,30)(113,18)(461,3)(440,30)(240,6)(401,3)(410,9)(194,30)(478,15)(447,12)(129,12)(257,12)");
    timescalesList.add("(104,36)(296,36)(301,28)(245,28)(87,24)(379,12)(288,32)(476,16)(414,24)(260,20)(256,4)(69,12)(451,4)(453,12)(234,24)(103,8)(497,40)(402,36)(180,40)(113,24)(461,4)(440,40)(240,8)(401,4)(410,12)(194,40)(478,20)(447,16)(129,16)(257,16)");
    timescalesList.add("(104,45)(296,45)(301,35)(245,35)(87,30)(379,15)(288,40)(476,20)(414,30)(260,25)(256,5)(69,15)(451,5)(453,15)(234,30)(103,10)(497,50)(402,45)(180,50)(113,30)(461,5)(440,50)(240,10)(401,5)(410,15)(194,50)(478,25)(447,20)(129,20)(257,20)");

    final List<Integer> multiples = Arrays.asList(1, 2, 3, 4, 5);
    final ProcessHelper processHelper = new ProcessHelper();

    for (final TestRunner.OperatorType operatorType : operatorTypes) {
      final Injector injector = Tang.Factory.getTang().newInjector();
      final OutputWriter writer = injector.getInstance(LocalOutputWriter.class);
      writer.writeLine(outputPath + "/result", "\n----------------------" + operatorType.name() + "-------------\n" +
          "IntervalRatio\tPartialCnt\tFinalCnt\tElapsedTime\n");
      // End of experiments
      Thread.sleep(2000);
      writer.close();
      int i = 0;
      for (final String timescales : timescalesList) {
        if (!(operatorType.equals(TestRunner.OperatorType.PAFAS_DP) && i < 3)) {
          final Process process = processHelper.startNewJavaProcess("-Xms22000m -Xmx56000m",
              "vldb.evaluation.WikiWordCountEvaluation",
              "./target/tempest-0.11-SNAPSHOT.jar",
              EvaluationUtils.getCommand(operatorType, inputRate, outputPath, timescales, numThreads, multiples.get(i)+"")
          );

          StreamGobbler errorGobbler = new StreamGobbler(process.getErrorStream());
          StreamGobbler outputGobbler = new StreamGobbler(process.getInputStream());
          errorGobbler.start();
          outputGobbler.start();
          process.waitFor();
          /*
          final Process p = Runtime.getRuntime().exec(
              EvaluationUtils.getCommand(operatorType, inputRate, outputPath, timescales, numThreads, multiples.get(i) + ""));
          p.waitFor();
          */
        }
        i++;
      }
    }

    /*
    final Configuration commandLineConf = getCommandLineConf(args);

    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder(commandLineConf);
    cb.bindNamedParameter(RandomSlidingWindowGenerator.MinWindowSize.class, minWindowSize + "");
    cb.bindNamedParameter(RandomSlidingWindowGenerator.MaxWindowSize.class, maxWindowSize + "");
    cb.bindNamedParameter(RandomSlidingWindowGenerator.MinIntervalSize.class, minIntervalSize + "");
    cb.bindNamedParameter(RandomSlidingWindowGenerator.MaxIntervalSize.class, maxIntervalSize + "");
    final Injector injector = Tang.Factory.getTang().newInjector(cb.build());

    SlidingWindowGenerator swg = injector.getInstance(RandomSlidingWindowGenerator.class);

    final OutputWriter writer = injector.getInstance(LocalOutputWriter.class);

    // Print configuration
    final List<TestRunner.OperatorType> operatorTypes = Arrays.asList(
        OperatorType.PAFAS,
        OperatorType.PAFAS_DP,
        OperatorType.TriOps,
        OperatorType.OnTheFly,
        TestRunner.OperatorType.Naive
    );

    // Experiment3: Change the interval size
    writer.writeLine(outputPath+"/result", "============ [Experiment3]: Change the interval size =============");
    writer.writeLine(outputPath+"/result", "!windowRange=[" + minWindowSize + "-" + maxWindowSize + "]");
    writer.writeLine(outputPath+"/result", "!intervalRange=[" + minIntervalSize + "-" + maxIntervalSize + "]");
    writer.writeLine(outputPath+"/result", "!Dataset=WikiWordGenerator");
    writer.writeLine(outputPath+"/result", "!InputRate=" + inputRate);

    final JavaConfigurationBuilder jcb2 = Tang.Factory.getTang().newConfigurationBuilder();
    jcb2.bindNamedParameter(RandomSlidingWindowGenerator.MinWindowSize.class, minWindowSize + "");
    jcb2.bindNamedParameter(RandomSlidingWindowGenerator.MaxWindowSize.class, maxWindowSize + "");
    jcb2.bindNamedParameter(RandomSlidingWindowGenerator.MinIntervalSize.class, minIntervalSize + "");
    jcb2.bindNamedParameter(RandomSlidingWindowGenerator.MaxIntervalSize.class, maxIntervalSize + "");
    final Injector injector2 = Tang.Factory.getTang().newInjector(jcb2.build());
    swg = injector2.getInstance(RandomSlidingWindowGenerator.class);

    final List<Timescale> exp3Timescale = swg.generateSlidingWindows(30);
    final List<Integer> intervalMultiples = Arrays.asList(1, 2, 3, 4, 5);
    final List<List<Timescale>> timescalesList = new LinkedList<>();
    for (final int multiple : intervalMultiples) {
      final List<Timescale> timescales = new LinkedList<>();
      for (final Timescale ts : exp3Timescale) {
        timescales.add(new Timescale(ts.windowSize, ts.intervalSize*multiple));
      }
      timescalesList.add(timescales);
      writer.writeLine(outputPath+"/result", "!Window=" + TimescaleParser.parseToString(timescales));
    }

    for (final OperatorType operatorType : operatorTypes) {
      writer.writeLine(outputPath+"/result", "\n----------------------" + operatorType.name() + "-------------\n" +
          "IntervalRatio\tPartialCnt\tFinalCnt\tElapsedTime\n");
      int i = 0;
      for (final int multiple : intervalMultiples) {
        final Result result = runTest(timescalesList.get(i), writer, outputPath, multiple + "",
            numThreads, wikiDataPath, operatorType, inputRate);
        writer.writeLine(outputPath+"/result", multiple + "\t" + result.partialCount + "\t" + result.finalCount + "\t" + result.elapsedTime);
        i += 1;
      }
    }

    */

  }
}
