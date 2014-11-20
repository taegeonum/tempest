package edu.snu.org;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.CommandLine;

import backtype.storm.Config;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import edu.snu.org.util.StormRunner;
import edu.snu.org.util.Timescale;

public class WordCountApp {
  @NamedParameter(short_name = "local", default_value = "true")
  public static final class Local implements Name<Boolean> {}
  
  @NamedParameter(short_name = "app_name", default_value = "WordcountAppMTSS")
  public static final class AppName implements Name<String> {}
  
  @NamedParameter(short_name = "num_workers", default_value = "1")
  public static final class NumWorkers implements Name<Integer> {}
  
  @NamedParameter(doc = "input interval (ms)", short_name = "input_interval", default_value = "10")
  public static final class InputInterval implements Name<Integer> {}
  
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
    .registerShortNameOfClass(NumSpout.class)
    .registerShortNameOfClass(NumBolt.class)
    .registerShortNameOfClass(TopN.class)
    .registerShortNameOfClass(Runtime.class)
    .processCommandLine(args);

    return cl.getBuilder().build();
  }
  
  
  public static void main(String[] args) throws InterruptedException, AlreadyAliveException, InvalidTopologyException, AuthorizationException, BindException, IOException, InjectionException {
    String topologyName = "slidingWindowCounts";
    
    Configuration commandLineConf = getCommandLineConf(args);
    final Injector commandLineInjector = Tang.Factory.getTang().newInjector(commandLineConf);
    final boolean isLocal = commandLineInjector.getNamedInstance(Local.class);
    final int numWorkers = commandLineInjector.getNamedInstance(NumWorkers.class);
    final int runtime = commandLineInjector.getNamedInstance(Runtime.class);
    String appName = commandLineInjector.getNamedInstance(AppName.class);
    
    Config conf = createTopologyConfiguration(isLocal, numWorkers);
    
    JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder(commandLineConf);
    cb.bindNamedParameter(TimescaleList.class, TimescaleClass.class);
    cb.bindImplementation(AppTopologyBuilder.class, TopologyBuilderClassFactory.createTopologyBuilderClass(appName));
    
    Configuration c = cb.build();
    Injector ij = Tang.Factory.getTang().newInjector(c);
    AppTopologyBuilder topologyBuilder = ij.getInstance(AppTopologyBuilder.class);
    
    if (isLocal) {
      StormRunner.runTopologyLocally(topologyBuilder.createTopology(), topologyName, conf, runtime);
    }
    else {
      StormRunner.runTopologyRemotely(topologyBuilder.createTopology(), topologyName, conf);
    }
    
  }
  
  public static final class TimescaleClass {
    
    public List<Timescale> timescales;
    
    @Inject
    public TimescaleClass() {
      List<Timescale> timescales = new ArrayList<>();

      
      timescales.add(new Timescale(30, 2, TimeUnit.SECONDS, TimeUnit.SECONDS));
      timescales.add(new Timescale(60, 5, TimeUnit.SECONDS, TimeUnit.SECONDS));
      timescales.add(new Timescale(90, 6, TimeUnit.SECONDS, TimeUnit.SECONDS));
      timescales.add(new Timescale(120, 10, TimeUnit.SECONDS, TimeUnit.SECONDS));
      timescales.add(new Timescale(180, 12, TimeUnit.SECONDS, TimeUnit.SECONDS));
      timescales.add(new Timescale(210, 15, TimeUnit.SECONDS, TimeUnit.SECONDS));
      
      timescales.add(new Timescale(300, 20, TimeUnit.SECONDS, TimeUnit.SECONDS));
      
      timescales.add(new Timescale(600, 25, TimeUnit.SECONDS, TimeUnit.SECONDS));
      timescales.add(new Timescale(1000, 30, TimeUnit.SECONDS, TimeUnit.SECONDS));
      
      this.timescales = timescales;
    }
  }
}
