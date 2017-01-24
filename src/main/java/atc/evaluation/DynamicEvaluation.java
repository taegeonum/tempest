package atc.evaluation;

import edu.snu.tempest.example.util.writer.LocalOutputWriter;
import edu.snu.tempest.example.util.writer.OutputWriter;
import org.apache.reef.tang.*;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.formats.AvroConfigurationSerializer;
import org.apache.reef.tang.formats.CommandLine;
import atc.evaluation.parameter.EndTime;
import atc.evaluation.parameter.OutputPath;
import atc.evaluation.parameter.TestName;
import atc.evaluation.parameter.Variable;
import atc.evaluation.util.LoggingHandler;
import atc.example.DefaultExtractor;
import atc.operator.window.aggregator.impl.CountByKeyAggregator;
import atc.operator.window.aggregator.impl.KeyExtractor;
import atc.operator.window.timescale.TimeWindowOutputHandler;
import atc.operator.window.timescale.Timescale;
import atc.operator.window.timescale.common.TimescaleParser;
import atc.operator.window.timescale.pafas.dynamic.*;
import atc.operator.window.timescale.pafas.event.WindowTimeEvent;
import atc.operator.window.timescale.parameter.NumThreads;
import atc.operator.window.timescale.profiler.AggregationCounter;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Created by taegeonum on 4/25/16.
 */
public final class DynamicEvaluation {
  private static final Logger LOG = Logger.getLogger(DynamicEvaluation.class.getName());
  //static final String twitterDataPath = "./dataset/bigtwitter.txt";

  @NamedParameter(short_name = "total_ts")
  public static class TotalTS implements Name<String> {

  }

  /**
   * Parse command line arguments.
   */
  private static Configuration getCommandLineConf(String[] args) throws BindException, IOException {
    final Tang tang = Tang.Factory.getTang();
    final JavaConfigurationBuilder cb = tang.newConfigurationBuilder();

    final CommandLine cl = new CommandLine(cb)
        .registerShortNameOfClass(OutputPath.class)
        .registerShortNameOfClass(Variable.class)
        .registerShortNameOfClass(EndTime.class)
        .registerShortNameOfClass(TestName.class)
        .registerShortNameOfClass(TotalTS.class)
            .processCommandLine(args);

    return cl.getBuilder().build();
  }


  public static void main(final String[] args) throws Exception {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindImplementation(KeyExtractor.class, DefaultExtractor.class);
    jcb.bindNamedParameter(NumThreads.class, "4");

    final Configuration commandLineConf = getCommandLineConf(args);
    final Injector ij = Tang.Factory.getTang().newInjector(commandLineConf);

    final String timescaleString = ij.getNamedInstance(TotalTS.class);
    final String outputPath = ij.getNamedInstance(OutputPath.class);
    final OutputWriter writer = ij.getInstance(LocalOutputWriter.class);
    final String testName = ij.getNamedInstance(TestName.class);
    final long endTime = ij.getNamedInstance(EndTime.class);

    final List<Timescale> timescaleList = TimescaleParser.parseFromStringNoSort(timescaleString);

    final Configuration conf = DynamicMWOConfiguration.CONF
        .set(DynamicMWOConfiguration.INITIAL_TIMESCALES, timescaleList.get(0).toString())
        .set(DynamicMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
        .set(DynamicMWOConfiguration.DYNAMIC_DEPENDENCY, DynamicOptimizedDependencyGraphImpl.class)
        .set(DynamicMWOConfiguration.OUTPUT_LOOKUP_TABLE, DynamicDPOutputLookupTableImpl.class)
        .set(DynamicMWOConfiguration.SELECTION_ALGORITHM, DynamicDPSelectionAlgorithm.class)
        .set(DynamicMWOConfiguration.DYNAMIC_PARTIAL, DynamicOptimizedPartialTimespans.class)
        .set(DynamicMWOConfiguration.START_TIME, 0)
        .build();

    final Injector injector = Tang.Factory.getTang().newInjector(Configurations.merge(jcb.build(), conf));

    injector.bindVolatileInstance(TimeWindowOutputHandler.class, new LoggingHandler<>("DynamicMWO"));
    final MultiThreadDynamicMWO<Object, Map<String, Long>> mwo = injector.getInstance(MultiThreadDynamicMWO.class);
    final AggregationCounter aggregationCounter = injector.getInstance(AggregationCounter.class);

    final AvroConfigurationSerializer serializer = injector.getInstance(AvroConfigurationSerializer.class);

    final String prefix = outputPath + testName;
    writer.writeLine(prefix + "_result", "-------------------------------------\n"
        + serializer.toString(commandLineConf) + "--------------------------------");

    final int numKey = 1000;
    final Random random = new Random();
    long tick = 1;
    long input = 1;
    while (tick <= endTime) {
      final int key = Math.abs(random.nextInt() % numKey);
      if (input % 10 == 0) {
        //System.out.println("Tick " + tick);
        mwo.execute(new WindowTimeEvent(tick));

        // Add window
        if (tick < timescaleList.size() * 10 && tick % 10 == 0) {
          final int index = (int)tick/10;
          final Timescale ts = timescaleList.get(index);
          final long addStartTime = System.nanoTime();
          mwo.addWindow(ts, tick);
          final long addEndTime = System.nanoTime();
          final long elapsed = TimeUnit.NANOSECONDS.toMillis(addEndTime - addStartTime);
          writer.writeLine(prefix + "_result", "ADD\t" + ts + "\t" + elapsed);
        }

        if (tick > timescaleList.size() * 10 && tick <= timescaleList.size() * 2 * 10 && tick % 10 == 0) {
          int index = timescaleList.size() - 1 - ((int)(tick/10)%10);
          final Timescale ts = timescaleList.get(index);
          final long rmStartTime = System.nanoTime();
          mwo.removeWindow(ts, tick);
          final long rmEndTime = System.nanoTime();
          final long elapsed = TimeUnit.NANOSECONDS.toMillis(rmEndTime - rmStartTime);
          writer.writeLine(prefix + "_result", "REMOVE\t" + ts + "\t" + elapsed);
        }
        tick += 1;
      } else {
        mwo.execute(Integer.toString(key));
      }
      input += 1;
    }
    mwo.close();
    Runtime.getRuntime().halt(1);
  }
}
