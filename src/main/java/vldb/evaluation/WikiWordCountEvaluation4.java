package vldb.evaluation;

import edu.snu.tempest.example.util.writer.LocalOutputWriter;
import edu.snu.tempest.example.util.writer.OutputWriter;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.formats.CommandLine;
import vldb.evaluation.util.RandomSlidingWindowGenerator;
import vldb.evaluation.util.SlidingWindowGenerator;
import vldb.operator.window.timescale.Timescale;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;

/**
 * Created by taegeonum on 4/25/16.
 */
public final class WikiWordCountEvaluation4 {
  private static final Logger LOG = Logger.getLogger(WikiWordCountEvaluation4.class.getName());

  /**
   * Parameters.
   */
  static final int minWindowSize = 30;
  static final int maxWindowSize = 600;
  static final int minIntervalSize = 1;
  static final int maxIntervalSize = 10;
  static final int numThreads = 16;
  static final double inputRate = 20000;
  static final String wikiDataPath = "./dataset/big_wiki.txt";
  static final String outputPath = "./log/0428-wordcount/input-rate";

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
        TestRunner.OperatorType.OnTheFly
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
    Collections.sort(exp4Timescale);
    final List<Double> inputRates = Arrays.asList(1000.0, 5000.0, 10000.0, 15000.0, 20000.0, 25000.0, 30000.0);
    for (final TestRunner.OperatorType operatorType : operatorTypes) {
      writer.writeLine(outputPath+"/result", "\n----------------------" + operatorType.name() + "-------------\n");
      for (final double inputRate : inputRates) {
        final TestRunner.Result result = TestRunner.runTest(exp4Timescale, writer, outputPath, inputRate+"",
            numThreads, wikiDataPath, operatorType, inputRate);
        writer.writeLine(outputPath+"/result", inputRate + "\t" + result.partialCount + "\t" + result.finalCount + "\t" + result.elapsedTime);
      }
    }


    // End of experiments
    Thread.sleep(2000);
    writer.close();
  }
}
