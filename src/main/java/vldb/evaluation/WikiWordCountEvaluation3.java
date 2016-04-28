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
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

import static vldb.evaluation.TestRunner.*;

/**
 * Created by taegeonum on 4/25/16.
 */
public final class WikiWordCountEvaluation3 {
  private static final Logger LOG = Logger.getLogger(WikiWordCountEvaluation3.class.getName());

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
  static final String outputPath = "./log/0428-wordcount/interval-size";

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
        OperatorType.PAFAS,
        OperatorType.PAFAS_DP,
        OperatorType.TriOps,
        OperatorType.OnTheFly
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
    for (final OperatorType operatorType : operatorTypes) {
      writer.writeLine(outputPath+"/result", "\n----------------------" + operatorType.name() + "-------------\n");
      for (final int multiple : intervalMultiples) {
        final List<Timescale> ts = new LinkedList<>();
        for (final Timescale timescale : exp3Timescale) {
          ts.add(new Timescale(timescale.windowSize, timescale.intervalSize * multiple));
        }
        final Result result = runTest(ts, writer, outputPath, multiple + "",
            numThreads, wikiDataPath, operatorType, inputRate);
        writer.writeLine(outputPath+"/result", multiple + "\t" + result.partialCount + "\t" + result.finalCount + "\t" + result.elapsedTime);
      }
    }

    // End of experiments
    Thread.sleep(2000);
    writer.close();
  }
}
