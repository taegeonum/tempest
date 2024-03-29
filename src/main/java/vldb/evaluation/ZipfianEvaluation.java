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
import vldb.evaluation.parameter.*;
import vldb.evaluation.util.Profiler;
import vldb.operator.window.timescale.Timescale;
import vldb.operator.window.timescale.common.TimescaleParser;
import vldb.operator.window.timescale.parameter.NumThreads;
import vldb.operator.window.timescale.parameter.TimescaleString;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Created by taegeonum on 4/25/16.
 */
public final class ZipfianEvaluation {
  private static final Logger LOG = Logger.getLogger(ZipfianEvaluation.class.getName());
  static final String wikiDataPath = "./dataset/big_wiki.txt";

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
        .registerShortNameOfClass(NumThreads.class)
        .registerShortNameOfClass(InputRate.class)
        .registerShortNameOfClass(Variable.class)
        .registerShortNameOfClass(EndTime.class)
        .registerShortNameOfClass(TestName.class)
        .registerShortNameOfClass(NumOfKey.class)
        .processCommandLine(args);

    return cl.getBuilder().build();
  }


  public static void main(final String[] args) throws Exception {

    final Configuration commandLineConf = getCommandLineConf(args);
    final Injector injector = Tang.Factory.getTang().newInjector(commandLineConf);

    // Parameters
    final String outputPath = injector.getNamedInstance(OutputPath.class);
    final List<Timescale> timescales = TimescaleParser.parseFromString(
        injector.getNamedInstance(TimescaleString.class));
    final int numThreads = injector.getNamedInstance(NumThreads.class);
    final double inputRate = injector.getNamedInstance(InputRate.class);
    final String variable = injector.getNamedInstance(Variable.class);
    final long endTime = injector.getNamedInstance(EndTime.class);
    final String testName = injector.getNamedInstance(TestName.class);
    final long numKey = injector.getNamedInstance(NumOfKey.class);

    final TestRunner.OperatorType operatorType = TestRunner.OperatorType.valueOf(
        injector.getNamedInstance(OperatorTypeParam.class));

    final OutputWriter writer = injector.getInstance(LocalOutputWriter.class);
    final AvroConfigurationSerializer serializer = injector.getInstance(AvroConfigurationSerializer.class);

    final String prefix = outputPath + testName + "-" + variable + "-" + operatorType.name();
    writer.writeLine(prefix + "_result", "-------------------------------------\n"
        + serializer.toString(commandLineConf) + "--------------------------------");

    // Profiler
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

    /*
    final TestRunner.Result result = TestRunner.runTest(timescales,
        numThreads, numKey, operatorType, inputRate, endTime, writer, prefix);
    writer.writeLine(prefix + "_result", operatorType.name() + "\t" + variable + "\t" + result.partialCount + "\t" + result.finalCount + "\t" + result.elapsedTime);
    */

    // End of experiments
    Thread.sleep(2000);
    executorService.shutdownNow();
    writer.close();
    //System.exit(1);
    Runtime.getRuntime().halt(1);
  }
}
