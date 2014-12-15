package edu.snu.org;

import java.io.File;
import java.io.IOException;
import java.security.InvalidParameterException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

import org.apache.commons.lang.StringUtils;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.formats.CommandLine;

import backtype.storm.Config;
import backtype.storm.topology.base.BaseRichSpout;
import edu.snu.org.AppTopologyBuilder.OutputDir;
import edu.snu.org.util.HDFSOutputWriter;
import edu.snu.org.util.InputReader;
import edu.snu.org.util.LocalOutputWriter;
import edu.snu.org.util.StormRunner;
import edu.snu.org.util.Timescale;

public class TestApp {
  private static final Logger LOG = Logger.getLogger(TestApp.class.getName());

  @NamedParameter(short_name = "local", default_value = "true")
  public static final class Local implements Name<Boolean> {}
  
  @NamedParameter(short_name = "app_name", default_value = "NaiveTopology")
  public static final class AppName implements Name<String> {}
  
  @NamedParameter(short_name = "num_workers", default_value = "1")
  public static final class NumWorkers implements Name<Integer> {}
  
  @NamedParameter(doc = "input interval (ms)", short_name = "input_interval", default_value = "10")
  public static final class InputInterval implements Name<Double> {}
  
  @NamedParameter(doc = "The number of parallel spout", short_name = "num_spout", default_value = "4")
  public static final class NumSpout implements Name<Integer> {}
  
  @NamedParameter(doc = "The number of parallel bolt", short_name = "num_bolt", default_value = "1")
  public static final class NumBolt implements Name<Integer> {}
  
  @NamedParameter(doc = "top n", short_name = "topN", default_value = "10")
  public static final class TopN implements Name<Integer> {}
  
  @NamedParameter(doc = "runtime (Sec)", short_name = "runtime", default_value = "300") 
  public static final class Runtime implements Name<Integer> {}
  
  @NamedParameter
  public static final class TimescaleList implements Name<TimescaleClass> {}
  
  @NamedParameter(doc = "timescales. format: (\\(\\d+,\\d+\\))*. TimeUnit: sec",short_name = "timescales", default_value = "(30,2)(60,5)(90,6)")
  public static final class TimescaleParameter implements Name<String> {}
  
  @NamedParameter(doc = "Spout name", short_name = "spout", default_value = "RandomWordSpout") 
  public static final class SpoutName implements Name<String> {}

  @NamedParameter(doc = "output directory", short_name = "output")
  public static final class OutputDirFromArg implements Name<String> {}

  @NamedParameter(doc = "input file path", short_name = "input")
  public static final class InputFilePath implements Name<String> {}
  
  private static Config createTopologyConfiguration(Boolean isLocal, Integer numWorker) {
    Config conf = new Config();
    conf.put(Config.TOPOLOGY_DEBUG, false);
    
    if (!isLocal) {
      conf.setNumWorkers(numWorker);
      conf.setNumAckers(numWorker);
    }

    conf.setDebug(false);
    return conf;
  }
  
  private static Configuration getCommandLineConf(String[] args) throws BindException, IOException {

    final Tang tang = Tang.Factory.getTang();
    final JavaConfigurationBuilder cb = tang.newConfigurationBuilder();

    CommandLine cl = new CommandLine(cb)
    .registerShortNameOfClass(Local.class)
    .registerShortNameOfClass(AppName.class)
    .registerShortNameOfClass(NumWorkers.class)
    .registerShortNameOfClass(InputInterval.class)
    .registerShortNameOfClass(NumSpout.class)
    .registerShortNameOfClass(NumBolt.class)
    .registerShortNameOfClass(TopN.class)
    .registerShortNameOfClass(Runtime.class)
    .registerShortNameOfClass(SpoutName.class)
    .registerShortNameOfClass(InputFilePath.class)
    .registerShortNameOfClass(TimescaleParameter.class) 
    .registerShortNameOfClass(OutputDirFromArg.class)
    .processCommandLine(args);

    return cl.getBuilder().build();
  }
  
  
  public static void main(String[] args) throws Exception {
    String topologyName = "slidingWindowCounts";
    
    Configuration commandLineConf = getCommandLineConf(args);
    final Injector commandLineInjector = Tang.Factory.getTang().newInjector(commandLineConf);
    final boolean isLocal = commandLineInjector.getNamedInstance(Local.class);
    final int numWorkers = commandLineInjector.getNamedInstance(NumWorkers.class);
    final int runtime = commandLineInjector.getNamedInstance(Runtime.class);
    String appName = commandLineInjector.getNamedInstance(AppName.class);
    String spoutName = commandLineInjector.getNamedInstance(SpoutName.class);
    String outputDir = commandLineInjector.getNamedInstance(OutputDirFromArg.class);
    long timestamp = System.currentTimeMillis();
    
    // create output directory name 
    SimpleDateFormat sdf = new SimpleDateFormat("MMddyy-HH-mm-ss");
    Date resultdate = new Date(timestamp);
    String dir = outputDir + sdf.format(resultdate) + "-storm-result/";
    
    Config conf = createTopologyConfiguration(isLocal, numWorkers);
    
    JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder(commandLineConf);
    cb.bindNamedParameter(TimescaleList.class, TimescaleClass.class);
    cb.bindImplementation(AppTopologyBuilder.class, ClassFactory.createTopologyBuilderClass(appName));
    cb.bindImplementation(BaseRichSpout.class, ClassFactory.createSpoutClass(spoutName));
    cb.bindImplementation(InputReader.class, ClassFactory.createInputReaderClass(isLocal));
    cb.bindNamedParameter(OutputDir.class, dir);
    
    Configuration c = cb.build();
    Injector ij = Tang.Factory.getTang().newInjector(c);
    AppTopologyBuilder topologyBuilder = ij.getInstance(AppTopologyBuilder.class);
    
    if (isLocal) {
      // Create folder and logging configuration 
      createDirectory(dir);
      LocalOutputWriter writer = new LocalOutputWriter(dir + "params");
      writer.writeLine(StringUtils.join(args, "\n"));
      writer.close();
      StormRunner.runTopologyLocally(topologyBuilder.createTopology(), topologyName, conf, runtime);
    } else {
      // Logging configuration of this test
      HDFSOutputWriter writer = new HDFSOutputWriter(dir + "params");
      writer.writeLine(StringUtils.join(args, "\n"));
      writer.close();
      StormRunner.runTopologyRemotely(topologyBuilder.createTopology(), topologyName, conf);
    }
  }
  
  public static final class TimescaleClass {
    
    public final List<Timescale> timescales;
    private static final String regex = "(\\(\\d+,\\d+\\))*";
    
    @Inject
    public TimescaleClass(@Parameter(TimescaleParameter.class) String params) {
      
      if (!params.matches(regex)) {
        throw new InvalidParameterException("Invalid timescales: " + params + " The format should be " + regex);
      }
      
      this.timescales = parseToTimescaleList(params);
    }
    
    private List<Timescale> parseToTimescaleList(String params) {
      List<Timescale> timescales = new ArrayList<>();
      
      
      // (1,2)(3,4) -> 1,2)3,4)
      String trim = params.replace("(", "");
      
      // 1,2)3,4) -> [ "1,2" , "3,4" ] 
      String[] args = trim.split("\\)");
      
      for (String arg : args) {
        String[] windowAndInterval = arg.split(",");
        timescales.add(new Timescale(Integer.valueOf(windowAndInterval[0]), Integer.valueOf(windowAndInterval[1]), TimeUnit.SECONDS, TimeUnit.SECONDS));
      }
      
      
      LOG.log(Level.INFO, "Timescales: " + timescales);
      return timescales;
    }
    
  }
  
  
  private static final void createDirectory(String dir) {
    File theDir = new File(dir);

    // if the directory does not exist, create it
    if (!theDir.exists()) {
      LOG.log(Level.INFO, "creating directory: " + dir);
      theDir.mkdir();
      LOG.log(Level.INFO, dir +" directory is created");

    } else {
      LOG.log(Level.INFO, "Dir " + dir + " is already exist.");
    }
  }
}
