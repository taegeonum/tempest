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
import java.util.List;
import java.util.logging.Logger;

/**
 * Created by taegeonum on 4/25/16.
 */
public final class EvaluationLauncher4 {
  private static final Logger LOG = Logger.getLogger(EvaluationLauncher4.class.getName());

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
  static final String outputPath = "./log/0429-wordcount/input-rate";

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

    final String timescales = "(292,3)(362,5)(461,4)(112,1)(371,3)(445,7)(194,9)(451,9)(488,10)(391,9)(228,4)(420,4)(290,2)(490,10)(71,5)(268,7)(110,8)(176,10)(425,2)(72,1)(181,10)(309,8)(113,2)(214,3)(448,6)(252,2)(163,7)(195,6)(136,10)(196,6)";

    final List<Double> inputRates = Arrays.asList(1000.0, 5000.0, 10000.0, 15000.0, 20000.0, 25000.0);
    final ProcessHelper processHelper = new ProcessHelper();
    for (final TestRunner.OperatorType operatorType : operatorTypes) {
      final Injector injector = Tang.Factory.getTang().newInjector();
      final OutputWriter writer = injector.getInstance(LocalOutputWriter.class);
      writer.writeLine(outputPath + "/result", "\n----------------------" + operatorType.name() + "-------------\n" +
          "InputRate\tPartialCnt\tFinalCnt\tElapsedTime\n");
      // End of experiments
      Thread.sleep(2000);
      writer.close();
      int i = 0;
      for (final double inputRate : inputRates) {
        if (!(operatorType.equals(TestRunner.OperatorType.FAST_STATIC) && i < 3)) {
          final Process process = processHelper.startNewJavaProcess("-Xms22000m -Xmx56000m",
              "vldb.evaluation.WikiWordCountEvaluation",
              "./target/tempest-0.11-SNAPSHOT.jar",
              EvaluationUtils.getCommand(operatorType, inputRate, outputPath, timescales, numThreads, inputRates.get(i)+"")
          );

          StreamGobbler errorGobbler = new StreamGobbler(process.getErrorStream());
          StreamGobbler outputGobbler = new StreamGobbler(process.getInputStream());
          errorGobbler.start();
          outputGobbler.start();
          process.waitFor();
          /*
          final Process p = Runtime.getRuntime().exec(
              EvaluationUtils.getCommand(operatorType, inputRate, outputPath, timescales, numThreads, inputRates.get(i) + ""));
          p.waitFor();
          */
        }
        i++;
      }
    }

    /*
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

    // Experiment4: Change the input rate
    // Pick 20 number of windows
    writer.writeLine(outputPath+"/result", "============ [Experiment4]: Change the input rate =============");
    writer.writeLine(outputPath+"/result", "!windowRange=[" + minWindowSize + "-" + maxWindowSize + "]");
    writer.writeLine(outputPath+"/result", "!intervalRange=[" + minIntervalSize + "-" + maxIntervalSize + "]");
    writer.writeLine(outputPath+"/result", "!Dataset=WikiWordGenerator");

    final JavaConfigurationBuilder jcb3 = Tang.Factory.getTang().newConfigurationBuilder();
    jcb3.bindNamedParameter(RandomSlidingWindowGenerator.MinWindowSize.class, minWindowSize + "");
    jcb3.bindNamedParameter(RandomSlidingWindowGenerator.MaxWindowSize.class, maxWindowSize + "");
    jcb3.bindNamedParameter(RandomSlidingWindowGenerator.MinIntervalSize.class, minIntervalSize+"");
    jcb3.bindNamedParameter(RandomSlidingWindowGenerator.MaxIntervalSize.class, maxIntervalSize+"");
    final Injector injector3 = Tang.Factory.getTang().newInjector(jcb3.build());
    swg = injector3.getInstance(RandomSlidingWindowGenerator.class);

    final List<Timescale> exp4Timescale = swg.generateSlidingWindows(30);
    writer.writeLine(outputPath+"/result", "!Windows=" + TimescaleParser.parseToString(exp4Timescale));
    Collections.sort(exp4Timescale);
    final List<Double> inputRates = Arrays.asList(1000.0, 5000.0, 10000.0, 15000.0, 20000.0, 25000.0, 30000.0);
    for (final TestRunner.OperatorType operatorType : operatorTypes) {
      writer.writeLine(outputPath+"/result", "\n----------------------" + operatorType.name() + "-------------\n" +
          "InputRate\tPartialCnt\tFinalCnt\tElapsedTime\n");
      for (final double inputRate : inputRates) {
        final TestRunner.Result result = TestRunner.runTest(exp4Timescale, writer, outputPath, inputRate+"",
            numThreads, wikiDataPath, operatorType, inputRate);
        writer.writeLine(outputPath+"/result", inputRate + "\t" + result.partialCount + "\t" + result.finalCount + "\t" + result.elapsedTime);
      }
    }

    */

  }
}
