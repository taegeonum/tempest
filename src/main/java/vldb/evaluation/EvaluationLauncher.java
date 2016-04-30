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
public final class EvaluationLauncher {
  private static final Logger LOG = Logger.getLogger(EvaluationLauncher.class.getName());

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
  static final String outputPath = "./log/0429-wordcount/num-windows";

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
    //timescalesList.add("(390,6)(456,8)(262,6)(162,1)(103,3)(294,1)(304,9)(335,8)(265,2)(398,6)(118,7)(222,8)(163,10)(295,10)(420,6)(169,2)(425,2)(367,8)(114,4)(349,9)(316,2)(454,10)(221,1)(135,8)(263,4)(270,8)(266,4)(334,6)(374,9)(208,2)(371,1)(438,3)(410,4)(376,1)(226,10)(160,4)(357,6)(324,3)(399,9)(77,7)(490,2)(248,10)(50,4)(277,5)(278,5)(442,4)(155,5)(379,2)(382,2)(225,3)");
    timescalesList.add("(233,9)(74,8)(232,3)(368,9)(337,10)(369,9)(210,8)(336,4)(435,7)(433,5)(177,3)(181,7)(344,8)(318,7)(321,9)(249,1)(66,9)(383,5)(446,1)(192,3)(167,6)(138,5)(238,6)(82,9)(111,6)(52,10)(372,10)(495,4)(375,10)(467,3)(306,2)(474,9)(54,4)(409,5)(183,2)(60,6)(250,2)(326,10)(288,3)(355,5)");
    timescalesList.add("(297,7)(104,1)(245,9)(116,7)(185,9)(406,6)(243,1)(279,2)(123,5)(280,2)(320,9)(443,3)(96,8)(219,2)(482,8)(381,3)(260,9)(164,4)(107,10)(451,1)(489,6)(395,5)(148,3)(312,6)(154,7)(419,9)(194,8)(220,1)(166,9)(323,6)");
    timescalesList.add("(101,5)(72,6)(492,7)(299,4)(436,10)(240,5)(313,8)(120,6)(308,1)(117,1)(378,2)(412,4)(126,5)(450,9)(157,3)(130,8)(158,1)(198,9)(68,6)(387,5)");
    timescalesList.add("(67,4)(281,9)(186,9)(131,1)(453,3)(139,5)(57,1)(227,10)(500,7)(423,9)");

    final List<Integer> numWindows = Arrays.asList(/*50, */40, 30, 20, 10);
    final ProcessHelper processHelper = new ProcessHelper();
    for (final TestRunner.OperatorType operatorType : operatorTypes) {
      final Injector injector = Tang.Factory.getTang().newInjector();
      final OutputWriter writer = injector.getInstance(LocalOutputWriter.class);
      writer.writeLine(outputPath + "/result", "\n----------------------" + operatorType.name() + "-------------\n" +
          "NumWindows\tPartialCnt\tFinalCnt\tElapsedTime\n");
      Thread.sleep(1000);
      writer.close();
      int i = 0;
      for (final String timescales : timescalesList) {
        final Process process = processHelper.startNewJavaProcess("-Xms22000m -Xmx56000m",
            "vldb.evaluation.WikiWordCountEvaluation",
            "./target/tempest-0.11-SNAPSHOT.jar",
            EvaluationUtils.getCommand(operatorType, inputRate, outputPath, timescales, numThreads, numWindows.get(i)+"")
            );

        StreamGobbler errorGobbler = new StreamGobbler(process.getErrorStream());
        StreamGobbler outputGobbler = new StreamGobbler(process.getInputStream());
        errorGobbler.start();
        outputGobbler.start();
        process.waitFor();
        /*
        final Process p = Runtime.getRuntime().exec(
            EvaluationUtils.getCommand(operatorType, inputRate, outputPath, timescales, numThreads, numWindows.get(i)+""));

        p.waitFor();
        */
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
    writer.writeLine(outputPath+"/result", "============ [Experiment1]: Change the number of windows=============");
    writer.writeLine(outputPath+"/result", "!windowRange=[" + minWindowSize + "-" + maxWindowSize + "]");
    writer.writeLine(outputPath+"/result", "!intervalRange=[" + minIntervalSize + "-" + maxIntervalSize + "]");
    writer.writeLine(outputPath+"/result", "!Dataset=WikiWordGenerator");
    writer.writeLine(outputPath+"/result", "!InputRate=" + inputRate);

    // Experiment1: Change the number of windows
    final List<Integer> numWindows = Arrays.asList(50, 40, 30, 20, 10);
    final List<TestRunner.OperatorType> operatorTypes = Arrays.asList(
        TestRunner.OperatorType.PAFAS,
        TestRunner.OperatorType.PAFAS_DP,
        TestRunner.OperatorType.TriOps,
        TestRunner.OperatorType.OnTheFly,
        TestRunner.OperatorType.Naive
    );

    final List<List<Timescale>> timescalesList = new LinkedList<>();
    for (final int numWindow : numWindows) {
      final List<Timescale> timescales = swg.generateSlidingWindows(numWindow);
      timescalesList.add(timescales);
      writer.writeLine(outputPath+"/result", "!Window"+numWindow+"=" + TimescaleParser.parseToString(timescales));
    }

    for (final TestRunner.OperatorType operatorType : operatorTypes) {
      writer.writeLine(outputPath+"/result", "\n----------------------" + operatorType.name() + "-------------\n" +
          "NumWindows\tPartialCnt\tFinalCnt\tElapsedTime\n");
      for (final List<Timescale> timescales : timescalesList) {
        Collections.sort(timescales);
          final TestRunner.Result result = TestRunner.runTest(timescales, writer, outputPath, timescales.size()+"",
              numThreads, wikiDataPath, operatorType, inputRate);
        writer.writeLine(outputPath+"/result", timescales.size() + "\t" + result.partialCount + "\t" + result.finalCount + "\t" + result.elapsedTime);
      }
    }
    */
  }
}
