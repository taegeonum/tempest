package vldb.evaluation;

import edu.snu.tempest.example.util.writer.LocalOutputWriter;
import edu.snu.tempest.example.util.writer.OutputWriter;
import org.apache.reef.tang.*;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.formats.AvroConfigurationSerializer;
import org.apache.reef.tang.formats.CommandLine;
import vldb.evaluation.common.FileWordGenerator;
import vldb.evaluation.common.Generator;
import vldb.evaluation.parameter.*;
import vldb.evaluation.util.LoggingHandler;
import vldb.example.DefaultExtractor;
import vldb.operator.window.aggregator.impl.CountByKeyAggregator;
import vldb.operator.window.aggregator.impl.KeyExtractor;
import vldb.operator.window.timescale.TimeWindowOutputHandler;
import vldb.operator.window.timescale.pafas.dynamic.DynamicDPOutputLookupTableImpl;
import vldb.operator.window.timescale.pafas.dynamic.DynamicDPSelectionAlgorithm;
import vldb.operator.window.timescale.pafas.dynamic.MultiThreadDynamicMWO;
import vldb.operator.window.timescale.pafas.dynamic.DynamicMWOConfiguration;
import vldb.operator.window.timescale.pafas.event.WindowTimeEvent;
import vldb.operator.window.timescale.parameter.NumThreads;
import vldb.operator.window.timescale.parameter.TimescaleString;
import vldb.operator.window.timescale.profiler.AggregationCounter;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Created by taegeonum on 4/25/16.
 */
public final class DynamicIncrementalEvaluation {
  private static final Logger LOG = Logger.getLogger(DynamicIncrementalEvaluation.class.getName());
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
        .processCommandLine(args);

    return cl.getBuilder().build();
  }


  public static void main(final String[] args) throws Exception {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindImplementation(KeyExtractor.class, DefaultExtractor.class);
    jcb.bindNamedParameter(NumThreads.class, "4");

    final Configuration commandLineConf = getCommandLineConf(args);
    final Injector ij = Tang.Factory.getTang().newInjector(commandLineConf);

    final String timescaleString = ij.getNamedInstance(TimescaleString.class);
    final String outputPath = ij.getNamedInstance(OutputPath.class);
    final OutputWriter writer = ij.getInstance(LocalOutputWriter.class);
    final String testName = ij.getNamedInstance(TestName.class);
    final long endTime = ij.getNamedInstance(EndTime.class);
    final String variable = ij.getNamedInstance(Variable.class);

    final Configuration conf = DynamicMWOConfiguration.CONF
        .set(DynamicMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
        .set(DynamicMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
        .set(DynamicMWOConfiguration.SELECTION_ALGORITHM, DynamicDPSelectionAlgorithm.class)
        .set(DynamicMWOConfiguration.OUTPUT_LOOKUP_TABLE, DynamicDPOutputLookupTableImpl.class)
        .set(DynamicMWOConfiguration.START_TIME, 0)
        .build();

    final Injector injector = Tang.Factory.getTang().newInjector(Configurations.merge(jcb.build(), conf));
    final Generator wordGenerator = injector.getInstance(FileWordGenerator.class);

    injector.bindVolatileInstance(TimeWindowOutputHandler.class, new LoggingHandler<>("DynamicMWO"));
    final MultiThreadDynamicMWO<Object, Map<String, Long>> mwo = injector.getInstance(MultiThreadDynamicMWO.class);
    final AggregationCounter aggregationCounter = injector.getInstance(AggregationCounter.class);

    final AvroConfigurationSerializer serializer = injector.getInstance(AvroConfigurationSerializer.class);

    final String prefix = outputPath + testName + "-" + variable ;
    writer.writeLine(prefix + "_result", "-------------------------------------\n"
        + serializer.toString(commandLineConf) + "--------------------------------");

    long tick = 1;
    long input = 1;
    while (tick <= endTime) {
      final String word = wordGenerator.nextString();
      mwo.execute(word);
      input += 1;
      if (input % 1000 == 0) {
        //System.out.println("Tick " + tick);
        final long beforeTick = System.nanoTime();
        mwo.execute(new WindowTimeEvent(tick));
        final long afterTick = System.nanoTime();
        final long elapsed = TimeUnit.NANOSECONDS.toMicros(afterTick - beforeTick);
        writer.writeLine(prefix + "_result", "INC_TIME" + "\t" + elapsed);
        tick += 1;
      }
    }
    mwo.close();
    Runtime.getRuntime().halt(1);
  }
}
