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
public final class EvaluationLauncher2 {
  private static final Logger LOG = Logger.getLogger(EvaluationLauncher2.class.getName());

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
  static final String outputPath = "./log/0429-wordcount/window-size";

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
        TestRunner.OperatorType.TriOps,
        TestRunner.OperatorType.OnTheFly,
        TestRunner.OperatorType.Naive
    );

    final List<String> timescalesList = new LinkedList<>();
    timescalesList.add("(11,6)(38,1)(12,6)(15,8)(74,2)(47,7)(19,8)(52,5)(86,6)(84,4)(58,7)(90,4)(24,2)(26,4)(92,4)(27,2)(34,6)(39,8)(41,8)(36,2)(70,3)(75,7)(77,7)(78,7)(49,10)(43,2)(80,1)(55,2)(64,8)(37,7)");
    timescalesList.add("(22,6)(76,1)(24,6)(30,8)(148,2)(94,7)(38,8)(104,5)(172,6)(168,4)(116,7)(180,4)(48,2)(52,4)(184,4)(54,2)(68,6)(78,8)(82,8)(72,2)(140,3)(150,7)(154,7)(156,7)(98,10)(86,2)(160,1)(110,2)(128,8)(74,7)");
    timescalesList.add("(33,6)(114,1)(36,6)(45,8)(222,2)(141,7)(57,8)(156,5)(258,6)(252,4)(174,7)(270,4)(72,2)(78,4)(276,4)(81,2)(102,6)(117,8)(123,8)(108,2)(210,3)(225,7)(231,7)(234,7)(147,10)(129,2)(240,1)(165,2)(192,8)(111,7)");
    timescalesList.add("(44,6)(152,1)(48,6)(60,8)(296,2)(188,7)(76,8)(208,5)(344,6)(336,4)(232,7)(360,4)(96,2)(104,4)(368,4)(108,2)(136,6)(156,8)(164,8)(144,2)(280,3)(300,7)(308,7)(312,7)(196,10)(172,2)(320,1)(220,2)(256,8)(148,7)");
    timescalesList.add("(55,6)(190,1)(60,6)(75,8)(370,2)(235,7)(95,8)(260,5)(430,6)(420,4)(290,7)(450,4)(120,2)(130,4)(460,4)(135,2)(170,6)(195,8)(205,8)(180,2)(350,3)(375,7)(385,7)(390,7)(245,10)(215,2)(400,1)(275,2)(320,8)(185,7)");

    final List<Integer> multiples = Arrays.asList(1, 2, 3, 4, 5);
    final ProcessHelper processHelper = new ProcessHelper();

    for (final TestRunner.OperatorType operatorType : operatorTypes) {
      final Injector injector = Tang.Factory.getTang().newInjector();
      final OutputWriter writer = injector.getInstance(LocalOutputWriter.class);
      writer.writeLine(outputPath + "/result", "\n----------------------" + operatorType.name() + "-------------\n" +
          "WindowRatio\tPartialCnt\tFinalCnt\tElapsedTime\n");
      Thread.sleep(1000);
      writer.close();
      int i = 0;
      for (final String timescales : timescalesList) {
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
            EvaluationUtils.getCommand(operatorType, inputRate, outputPath, timescales, numThreads, multiples.get(i)+""));
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
    final List<TestRunner.OperatorType> operatorTypes = Arrays.asList(
        TestRunner.OperatorType.PAFAS,
        TestRunner.OperatorType.PAFAS_DP,
        TestRunner.OperatorType.TriOps,
        TestRunner.OperatorType.OnTheFly,
        TestRunner.OperatorType.Naive
    );

    // Experiment2: Change the window size
    writer.writeLine(outputPath+"/result", "============ [Experiment2]: Change the window size =============");
    writer.writeLine(outputPath+"/result", "!windowRange=[" + minWindowSize + "-" + maxWindowSize + "]");
    writer.writeLine(outputPath+"/result", "!intervalRange=[" + minIntervalSize + "-" + maxIntervalSize + "]");
    writer.writeLine(outputPath+"/result", "!Dataset=WikiWordGenerator");
    writer.writeLine(outputPath+"/result", "!InputRate=" + inputRate);
    final JavaConfigurationBuilder jcb1 = Tang.Factory.getTang().newConfigurationBuilder();
    jcb1.bindNamedParameter(RandomSlidingWindowGenerator.MinWindowSize.class, (minWindowSize/5)+"");
    jcb1.bindNamedParameter(RandomSlidingWindowGenerator.MaxWindowSize.class, (maxWindowSize/5)+"");
    jcb1.bindNamedParameter(RandomSlidingWindowGenerator.MinIntervalSize.class, minIntervalSize+"");
    jcb1.bindNamedParameter(RandomSlidingWindowGenerator.MaxIntervalSize.class, maxIntervalSize+"");
    final Injector injector1 = Tang.Factory.getTang().newInjector(jcb1.build());
    swg = injector1.getInstance(RandomSlidingWindowGenerator.class);
    final List<Integer> multiples = Arrays.asList(1, 2, 3, 4, 5);

    final List<Timescale> exp2Timescale = swg.generateSlidingWindows(30);
    final List<List<Timescale>> timescalesList = new LinkedList<>();
    for (final int multiple : multiples) {
      final List<Timescale> timescales = new LinkedList<>();
      for (final Timescale ts : exp2Timescale) {
        timescales.add(new Timescale(ts.windowSize*multiple, ts.intervalSize));
      }
      timescalesList.add(timescales);
      writer.writeLine(outputPath+"/result", "!Window=" + TimescaleParser.parseToString(timescales));
    }

    for (final TestRunner.OperatorType operatorType : operatorTypes) {
      writer.writeLine(outputPath+"/result", "\n----------------------" + operatorType.name() + "-------------\n" +
          "WindowRatio\tPartialCnt\tFinalCnt\tElapsedTime\n");
      int i = 0;
      for (final int multiple : multiples) {
        final TestRunner.Result result =
            TestRunner.runTest(timescalesList.get(i), writer, outputPath, multiple+"", numThreads, wikiDataPath, operatorType, inputRate);
        writer.writeLine(outputPath+"/result", multiple + "\t" + result.partialCount + "\t" + result.finalCount + "\t" + result.elapsedTime);
        i += 1;
      }
    }
    */
  }
}
