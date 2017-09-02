package vldb.evaluation;

import edu.snu.tempest.example.util.writer.LocalOutputWriter;
import edu.snu.tempest.example.util.writer.OutputWriter;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.formats.AvroConfigurationSerializer;
import org.apache.reef.tang.formats.CommandLine;
import vldb.evaluation.common.FileWordGenerator;
import vldb.evaluation.parameter.*;
import vldb.operator.window.timescale.Timescale;
import vldb.operator.window.timescale.common.TimescaleParser;
import vldb.operator.window.timescale.parameter.NumThreads;
import vldb.operator.window.timescale.parameter.ReusingRatio;
import vldb.operator.window.timescale.parameter.TimescaleString;
import vldb.operator.window.timescale.parameter.WindowGap;

import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;

/**
 * Created by taegeonum on 4/25/16.
 */
public final class TwitterEvaluation {
  private static final Logger LOG = Logger.getLogger(TwitterEvaluation.class.getName());
  //static final String twitterDataPath = "./dataset/bigtwitter.txt";

  /**
   * Parse command line arguments.
   */
  private static Configuration getCommandLineConf(String[] args) throws BindException, IOException {
    final Tang tang = Tang.Factory.getTang();
    final JavaConfigurationBuilder cb = tang.newConfigurationBuilder();

    final CommandLine cl = new CommandLine(cb)
        .registerShortNameOfClass(OperatorTypeParam.class)
        .registerShortNameOfClass(TimescaleString.class)
        .registerShortNameOfClass(OutputPath.class)
        .registerShortNameOfClass(FileWordGenerator.FileDataPath.class)
        .registerShortNameOfClass(NumThreads.class)
        .registerShortNameOfClass(InputRate.class)
        .registerShortNameOfClass(Variable.class)
        .registerShortNameOfClass(EndTime.class)
        .registerShortNameOfClass(TestName.class)
        .registerShortNameOfClass(ReusingRatio.class)
            .registerShortNameOfClass(TestRunner.WindowChangePeriod.class)
                .registerShortNameOfClass(WindowGap.class)
        //.registerShortNameOfClass(NumOfKey.class)
        .processCommandLine(args);

    return cl.getBuilder().build();
  }


  public static void main(final String[] args) throws Exception {

    final Configuration commandLineConf = getCommandLineConf(args);
    final Injector injector = Tang.Factory.getTang().newInjector(commandLineConf);

    // Parameters
    final String timescaleString = injector.getNamedInstance(TimescaleString.class);
     String outputPath = injector.getNamedInstance(OutputPath.class);
    final List<Timescale> timescales = TimescaleParser.parseFromString(timescaleString);
    final int numThreads = injector.getNamedInstance(NumThreads.class);
    final double inputRate = injector.getNamedInstance(InputRate.class);
    final String variable = injector.getNamedInstance(Variable.class);
    final long endTime = injector.getNamedInstance(EndTime.class);
    final String testName = injector.getNamedInstance(TestName.class);
    final String dataPath = injector.getNamedInstance(FileWordGenerator.FileDataPath.class);
    final int windowChangePeriod = injector.getNamedInstance(TestRunner.WindowChangePeriod.class);
    final double reusingRatio = injector.getNamedInstance(ReusingRatio.class);
    final int windowGap = injector.getNamedInstance(WindowGap.class);
    //final long numKey = injector.getNamedInstance(NumOfKey.class);

    final TestRunner.OperatorType operatorType = TestRunner.OperatorType.valueOf(
        injector.getNamedInstance(OperatorTypeParam.class));

    final OutputWriter writer = injector.getInstance(LocalOutputWriter.class);
    final AvroConfigurationSerializer serializer = injector.getInstance(AvroConfigurationSerializer.class);

    String prefix;
    outputPath = !outputPath.endsWith("/") ? outputPath + "/" : outputPath;
    if ((operatorType == TestRunner.OperatorType.FastSt || operatorType == TestRunner.OperatorType.FastDy
    || operatorType == TestRunner.OperatorType.FastEg || operatorType == TestRunner.OperatorType.FastRb)
        && windowGap > 0 || reusingRatio < 1.0) {
      if (windowGap > 0) {
        prefix = outputPath +  testName + "/" + variable + "/" + operatorType.name() + "/" + windowGap;
      } else {
        prefix = outputPath +  testName + "/" + variable + "/" + operatorType.name() + "/" + reusingRatio;
      }
    } else {
      prefix = outputPath + testName + "/" + variable + "/" + operatorType.name();
    }
    System.out.println("PREFIX: " + prefix);

    writer.writeLine(prefix + "_conf", "-------------------------------------\n"
        + serializer.toString(commandLineConf) + "--------------------------------");

    // Profiler*
    /*
    final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
    executorService.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        try {
          writer.writeLine(prefix + "_memory", System.currentTimeMillis() + "\t" + Profiler.getMemoryUsage());
          //writer.writeLine(outputPath+"/" + variable + "-" + operatorType.name() + "-computation", System.currentTimeMillis() + "\t" + aggregationCounter.getNumPartialAggregation() +"\t" + aggregationCounter.getNumFinalAggregation());
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }, 1, 1, TimeUnit.SECONDS);
*/

    TestRunner.Result result = null;
    /*
    if (operatorType == TestRunner.OperatorType.DYNAMIC_WINDOW_DP || operatorType == TestRunner.OperatorType.DYNAMIC_WINDOW_GREEDY
        || operatorType == TestRunner.OperatorType.DYNAMIC_NAIVE || operatorType == TestRunner.OperatorType.DYNAMIC_OnTheFly
        || operatorType == TestRunner.OperatorType.DYNAMIC_WINDOW_DP_SMALLADD) {
      result = TestRunner.runFileWordDynamicTest(timescaleString,
          numThreads, dataPath, operatorType, inputRate, endTime, writer, prefix, windowChangePeriod);
    } else {
      result = TestRunner.runFileWordTest(timescales,
          numThreads, dataPath, operatorType, inputRate, endTime, writer, prefix);
    }
    */
    final Metrics metrics = TestRunner.runFileWordTest(timescales,
        numThreads, dataPath, operatorType, inputRate, endTime, writer, prefix, reusingRatio, windowGap);

    //writer.writeLine(prefix + "_result", operatorType.name() + "\t" + variable + "\t" + result.partialCount + "\t" + result.finalCount + "\t" + result.processedInput + "\t" + result.elapsedTime + result.timeMonitor);
    writer.writeLine(prefix + "_result",
        (reusingRatio < 1.0 ? reusingRatio : operatorType.name()) + "\t" + metrics);


    // End of experiments
    Thread.sleep(2000);
    //executorService.shutdownNow();
    writer.close();
    Runtime.getRuntime().halt(1);
  }
}
