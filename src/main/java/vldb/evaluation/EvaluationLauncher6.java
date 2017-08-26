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
public final class EvaluationLauncher6 {
  private static final Logger LOG = Logger.getLogger(EvaluationLauncher6.class.getName());

  /**
   * Parameters.
   */
  static final int minWindowSize = 200;
  static final int maxWindowSize = 1200;
  static final int minIntervalSize = 5;
  static final int maxIntervalSize = 100;
  static final int numThreads = 16;
  static final double inputRate = 10000;
  static final String wikiDataPath = "./dataset/big_wiki.txt";
  static final String outputPath = "./log/0429-wordcount/num-windows-large3";

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
        //TestRunner.OperatorType.PAFAS,
        //TestRunner.OperatorType.PAFAS_DP,
        TestRunner.OperatorType.TriOps
        //TestRunner.OperatorType.OnTheFly,
        //TestRunner.OperatorType.Naive
    );

    final List<String> timescalesList = new LinkedList<>();
    //timescalesList.add("(390,6)(456,8)(262,6)(162,1)(103,3)(294,1)(304,9)(335,8)(265,2)(398,6)(118,7)(222,8)(163,10)(295,10)(420,6)(169,2)(425,2)(367,8)(114,4)(349,9)(316,2)(454,10)(221,1)(135,8)(263,4)(270,8)(266,4)(334,6)(374,9)(208,2)(371,1)(438,3)(410,4)(376,1)(226,10)(160,4)(357,6)(324,3)(399,9)(77,7)(490,2)(248,10)(50,4)(277,5)(278,5)(442,4)(155,5)(379,2)(382,2)(225,3)");
    //timescalesList.add("(103,10)(116,6)(151,8)(154,6)(156,6)(198,4)(257,7)(270,9)(305,7)(306,9)(313,8)(339,3)(353,3)(358,9)(360,5)(378,8)(381,2)(475,7)(518,3)(532,3)(547,2)(555,3)(558,8)(566,1)(578,1)(652,5)(696,3)(717,6)(723,2)(752,10)(757,4)(772,4)(806,3)(859,6)(880,1)(916,10)(924,9)(935,10)(947,7)(997,8)");
    //timescalesList.add("(83,7)(95,1)(133,10)(139,10)(164,5)(206,4)(215,3)(249,2)(270,9)(283,9)(326,4)(344,1)(347,1)(362,9)(369,7)(519,6)(524,9)(536,3)(553,10)(583,4)(585,3)(604,10)(617,7)(637,3)(658,10)(666,1)(678,1)(734,2)(765,2)(828,2)(946,2)(966,3)(967,4)(974,2)(995,10)(1012,8)(1018,6)(1046,3)(1051,8)(1062,2)(1073,8)(1087,2)(1088,1)(1103,10)(1120,9)(1147,10)(1163,3)(1171,8)(1194,3)(1200,9)");
    timescalesList.add("(58,2)(88,7)(92,2)(134,9)(140,10)(224,5)(242,5)(243,10)(258,5)(270,2)(282,7)(307,10)(308,2)(326,8)(336,1)(369,10)(377,6)(382,1)(383,5)(390,4)(404,6)(416,1)(420,8)(495,10)(500,1)(503,6)(513,1)(526,3)(569,3)(582,3)(593,9)(613,6)(631,10)(663,5)(664,5)(683,5)(696,6)(699,3)(704,1)(713,4)(728,8)(740,7)(749,6)(764,8)(779,7)(792,2)(858,10)(867,5)(869,9)(922,5)(923,4)(985,8)(986,1)(1012,5)(1025,6)(1034,4)(1097,2)(1114,2)(1160,9)(1198,4)");
    final List<Integer> numWindows = Arrays.asList(/*50, */60);
    final ProcessHelper processHelper = new ProcessHelper();
    final Injector ij = Tang.Factory.getTang().newInjector();
    final OutputWriter writer = ij.getInstance(LocalOutputWriter.class);
    writer.writeLine(outputPath + "/result", "Windows: " + timescalesList.get(0));
    writer.writeLine(outputPath + "/result", "window range: [50-1200)");
    writer.writeLine(outputPath + "/result", "interval range: [1-10)");
    writer.writeLine(outputPath + "/result", "input rate: 10000");

    for (final TestRunner.OperatorType operatorType : operatorTypes) {
      final Injector injector = Tang.Factory.getTang().newInjector();
      writer.writeLine(outputPath + "/result", "\n----------------------" + operatorType.name() + "-------------\n" +
          "NumWindows\tPartialCnt\tFinalCnt\tElapsedTime\n");
      Thread.sleep(1000);
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
    writer.close();

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
