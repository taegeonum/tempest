package evaluation.example.common;

import edu.snu.tempest.example.storm.parameter.NumSpouts;
import edu.snu.tempest.example.storm.parameter.TotalTime;
import evaluation.example.parameter.NumOfKey;
import evaluation.example.parameter.ZipfianConstant;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.CommandLine;

import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Created by taegeonum on 9/26/15.
 */
public final class InputGenerator {

  @NamedParameter(short_name = "input_file_gen_path", default_value = "./")
  public static final class InputFileGenPath implements Name<String> {}

  /**
   * Parse command line arguments.
   */
  private static Configuration getCommandLineConf(String[] args) throws BindException, IOException {
    final Tang tang = Tang.Factory.getTang();
    final JavaConfigurationBuilder cb = tang.newConfigurationBuilder();

    final CommandLine cl = new CommandLine(cb)
        .registerShortNameOfClass(NumOfKey.class)
        .registerShortNameOfClass(ZipfianConstant.class)
        .registerShortNameOfClass(NumSpouts.class)
        .registerShortNameOfClass(TotalTime.class)
        .processCommandLine(args);

    return cl.getBuilder().build();
  }

  public static void main(String[] args) throws IOException, InjectionException {
    final Configuration commandLineConf = getCommandLineConf(args);
    final Injector injector = Tang.Factory.getTang().newInjector(commandLineConf);
    final long numOfKey = injector.getNamedInstance(NumOfKey.class);
    final double zipfianConst = injector.getNamedInstance(ZipfianConstant.class);
    final int split = injector.getNamedInstance(NumSpouts.class);
    final int totalTime = injector.getNamedInstance(TotalTime.class);
    final String inputPath = injector.getNamedInstance(InputFileGenPath.class);

    final ZipfianGenerator zipfianGenerator = new ZipfianGenerator(numOfKey, zipfianConst);
    final Random random = new Random();

    final List<FileWriter> files = new LinkedList<>();
    for (int i = 0; i < split; i++) {
      final FileWriter fileWriter = new FileWriter(inputPath + "input" + i);
      files.add(fileWriter);
    }

    final long startTime = System.currentTimeMillis();
    while (TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - startTime) < totalTime) {
      for (final FileWriter writer : files) {
        writer.write(zipfianGenerator.nextString() + "\t" + random.nextDouble() * 100 + "\n");
      }
    }

    for (final FileWriter writer : files) {
      writer.close();
    }
  }
}
