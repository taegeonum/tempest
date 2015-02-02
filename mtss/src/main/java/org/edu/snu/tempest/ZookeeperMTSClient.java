package org.edu.snu.tempest;

import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.formats.CommandLine;
import org.edu.snu.tempest.signal.MTSSignalSender;
import org.edu.snu.tempest.signal.impl.ZkMTSParameters;
import org.edu.snu.tempest.signal.impl.ZkSignalSender;

import java.io.IOException;

/**
 * Zookeeper client for sending timescale to MTSWindowOperator.
 */
public class ZookeeperMTSClient {
  
  @NamedParameter(doc = "timescale window size(sec)", short_name = "w")
  public static final class WindowSize implements Name<Integer> {}
  
  @NamedParameter(doc = "timescale interval (sec)", short_name = "i") 
  public static final class Interval implements Name<Integer> {}
  
  @NamedParameter(doc = "type of signal", short_name = "type")
  public static final class TypeOfSignal implements Name<String> {}
  
  private static Configuration getCommandLineConf(String[] args) throws BindException, IOException {

    final Tang tang = Tang.Factory.getTang();
    final JavaConfigurationBuilder cb = tang.newConfigurationBuilder();

    CommandLine cl = new CommandLine(cb)
    .registerShortNameOfClass(ZkMTSParameters.OperatorIdentifier.class)
    .registerShortNameOfClass(ZkMTSParameters.ZkMTSNamespace.class)
    .registerShortNameOfClass(ZkMTSParameters.ZkServerAddress.class)
    .registerShortNameOfClass(WindowSize.class)
    .registerShortNameOfClass(Interval.class)
    .registerShortNameOfClass(TypeOfSignal.class)
    .processCommandLine(args);

    return cl.getBuilder().build();
  }
  
  public static void main(String[] args) throws Exception {
    
    Configuration commandConf = getCommandLineConf(args);
    JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.bindImplementation(MTSSignalSender.class, ZkSignalSender.class);
    cb.addConfiguration(commandConf);
    Injector ij = Tang.Factory.getTang().newInjector(cb.build());
    
    MTSSignalSender sender = ij.getInstance(MTSSignalSender.class);
    
    String type = ij.getNamedInstance(TypeOfSignal.class);
    
    if (type.equalsIgnoreCase("addition")) {
      sender.addTimescale(new Timescale(ij.getNamedInstance(WindowSize.class), ij.getNamedInstance(Interval.class)));

    } else if (type.equalsIgnoreCase("deletion")) {
      sender.removeTimescale(new Timescale(ij.getNamedInstance(WindowSize.class), ij.getNamedInstance(Interval.class)));
    } else {
      throw new RuntimeException("Invalid signal type: " + type);
    }
    
    sender.close();
  }
}
